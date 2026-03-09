#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>
#include <math.h>
#include "brand.h"
#include "hiredis.h"

typedef struct graph_parameters_t {
    int n_messages;
    char output_file[100];
} graph_parameters_t;


int g_counter = 0;
int flag_SIGINT = 0;

double *recv_times = NULL;
double *process_times = NULL;
double first_timestamp = 0.0;

void initialize_signals();
void handler_SIGINT(int exitStatus);
void initialize_parameters(graph_parameters_t *p, redisContext *c);
// void shutdown_process();

char NICKNAME[20] = "C-consumer";
redisReply *reply = NULL;
redisContext *redis_context = NULL;

// Get current time in seconds (monotonic)
double get_current_time() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

// Helper: Extract send_time from a Redis entry
static int extract_send_time_from_entry(redisReply *entry, double *send_time) {
    if (entry->elements < 2) return 0;
    
    redisReply *entry_dict = entry->element[1];
    if (entry_dict->type != REDIS_REPLY_ARRAY) return 0;
    
    // Find the 'time' field
    for (size_t j = 0; j < entry_dict->elements; j += 2) {
        if (entry_dict->element[j]->type == REDIS_REPLY_STRING &&
            strcmp(entry_dict->element[j]->str, "time") == 0) {
            
            if (j + 1 < entry_dict->elements &&
                entry_dict->element[j + 1]->type == REDIS_REPLY_STRING) {
                
                const char *time_data = entry_dict->element[j + 1]->str;
                *send_time = *(double *)time_data;
                return 1;
            }
            break;
        }
    }
    return 0;
}

// Helper: Read one message from Redis stream
static int read_from_redis_stream(char *last_id, char *entry_id_out, double *send_time) {
    redisReply *xread_reply = (redisReply *)redisCommand(redis_context, 
        "XREAD COUNT 1 BLOCK 0 STREAMS random %s", last_id);
    
    if (xread_reply == NULL) return 0;
    
    int success = 0;
    if (xread_reply->type == REDIS_REPLY_ARRAY && xread_reply->elements > 0) {
        redisReply *stream_data = xread_reply->element[0];
        if (stream_data->elements >= 2) {
            redisReply *entries = stream_data->element[1];
            if (entries->elements > 0) {
                redisReply *entry = entries->element[0];
                
                // Update last_id
                if (entry->elements >= 1 && entry->element[0]->type == REDIS_REPLY_STRING) {
                    strcpy(entry_id_out, entry->element[0]->str);
                    strcpy(last_id, entry->element[0]->str);
                }
                
                // Extract send time
                success = extract_send_time_from_entry(entry, send_time);
            }
        }
    }
    
    freeReplyObject(xread_reply);
    return success;
}

// Main consume loop
void consume_messages(graph_parameters_t *graph_parameters) {
    char last_id[50] = "0-0";
    char entry_id[50] = "0-0";
    int n_messages = graph_parameters->n_messages;
    recv_times = (double *)malloc(n_messages * sizeof(double));
    process_times = (double *)malloc(n_messages * sizeof(double));
    const int WARMUP_SAMPLES = 500;
    int warmup_count = 0;

    // Warmup phase
    while (warmup_count < WARMUP_SAMPLES && !flag_SIGINT) {
        double send_time = 0.0;

        if (!read_from_redis_stream(last_id, entry_id, &send_time)) {
            break;  // Something is off
        }

        redisReply *xdel_reply = (redisReply *)redisCommand(redis_context, "XDEL random %s", entry_id);
        if (xdel_reply != NULL) {
            freeReplyObject(xdel_reply);
        }

        warmup_count++;
    }

    // Measurement phase
    g_counter = 0;
    while (g_counter < n_messages && !flag_SIGINT) {
        double send_time = 0.0;

        if (!read_from_redis_stream(last_id, entry_id, &send_time)) {
            break;  // Something is off
        }

        double now = get_current_time();

        if (g_counter == 0) {
            first_timestamp = send_time;
        }

        recv_times[g_counter] = now;
        process_times[g_counter] = now - send_time;

        redisReply *xdel_reply = (redisReply *)redisCommand(redis_context, "XDEL random %s", entry_id);
        if (xdel_reply != NULL) {
            freeReplyObject(xdel_reply);
        }

        g_counter++;
    }
}

// Save results to file and print statistics
void terminate(graph_parameters_t *graph_parameters) {
    printf("Total messages processed: %d\n", g_counter);
    
    if (g_counter > 1) {
        // Calculate receive period statistics
        double sum_diff = 0.0, max_diff = 0.0, sum_sq_diff = 0.0;
        int max_idx = 0;
        int count = 0;
        
        for (int i = 1; i < g_counter; i++) {
            double period = recv_times[i] - recv_times[i-1];
            sum_diff += period;
            sum_sq_diff += period * period;
            if (period > max_diff) {
                max_diff = period;
                max_idx = i;
            }
            count++;
        }
        
        double avg_period_sec = sum_diff / count;
        double variance_period = (sum_sq_diff / count) - (avg_period_sec * avg_period_sec);
        double stddev_period_sec = sqrt(variance_period < 0 ? 0 : variance_period);
        
        // Convert to microseconds
        double avg_period_us = avg_period_sec * 1e6;
        double stddev_period_us = stddev_period_sec * 1e6;
        double max_period_us = max_diff * 1e6;
        
        printf("Average receive period (us): %.6f\n", avg_period_us);
        printf("Max receive period (us): %.6f, idx: %d\n", max_period_us, max_idx);
        printf("Std receive period (us): %.6f\n", stddev_period_us);
        
        // Calculate latency statistics
        double sum_latency = 0.0, max_latency = 0.0, sum_sq_latency = 0.0;
        int max_latency_idx = 0;
        
        for (int i = 0; i < g_counter; i++) {
            sum_latency += process_times[i];
            sum_sq_latency += process_times[i] * process_times[i];
            if (process_times[i] > max_latency) {
                max_latency = process_times[i];
                max_latency_idx = i;
            }
        }
        
        double avg_latency_sec = sum_latency / g_counter;
        double variance_latency = (sum_sq_latency / g_counter) - (avg_latency_sec * avg_latency_sec);
        double stddev_latency_sec = sqrt(variance_latency < 0 ? 0 : variance_latency);
        
        // Convert to microseconds
        double avg_latency_us = avg_latency_sec * 1e6;
        double stddev_latency_us = stddev_latency_sec * 1e6;
        double max_latency_us = max_latency * 1e6;
        
        // Calculate throughput
        double throughput = g_counter / (recv_times[g_counter-1] - first_timestamp) / 1e3;
        printf("Throughput (msg/ms): %.2f\n", throughput);
        
        // Save results to CSV file in same format as zeromq
        if (strlen(graph_parameters->output_file) > 0) {
            FILE *outfile = fopen(graph_parameters->output_file, "w");
            if (outfile != NULL) {
                fprintf(outfile, "Metric,Recv_period,Latency,Throughput\n");
                fprintf(outfile, "mean,%.6f,%.6f,%.6f\n", avg_period_us, avg_latency_us, throughput);
                fprintf(outfile, "std,%.6f,%.6f, \n", stddev_period_us, stddev_latency_us);
                fprintf(outfile, "max,%.6f,%.6f, \n", max_period_us, max_latency_us);
                fclose(outfile);
            } else {
                printf("Error: Could not open output file: %s\n", graph_parameters->output_file);
            }
        }
    }
    
    printf("Consumer stopped.\n");
    
    // Signal completion
    if (redis_context != NULL) {
        redisCommand(redis_context, "XADD test_completion * consumer done");
        redisFree(redis_context);
        redis_context = NULL;
    }
}

void initialize_signals() {
    signal(SIGINT, &handler_SIGINT);
}

void handler_SIGINT(int exitStatus) {
    flag_SIGINT++;
}

void initialize_parameters(graph_parameters_t *p, redisContext *c) {
    char SUPERGRAPH_ID[] = "0";
    const nx_json *supergraph_json = get_supergraph_json(c, reply, SUPERGRAPH_ID);
    
    if (supergraph_json == NULL) {
        emit_status(c, NICKNAME, NODE_FATAL_ERROR, "No supergraph found for initialization. Aborting.");
        exit(1);
    }

    p->n_messages = get_parameter_int(supergraph_json, NICKNAME, "n_messages");
    strcpy(p->output_file, get_parameter_string(supergraph_json, NICKNAME, "output_file"));
}

// void shutdown_process() {
//     printf("[%s] SIGINT received. Shutting down.\n", NICKNAME);
//     terminate(&g_graph_parameters);
//     exit(0);
// }

int main(int argc, char *argv[]) {
    
    redis_context = parse_command_line_args_init_redis(argc, argv, NICKNAME);
    
    initialize_signals();

    graph_parameters_t graph_parameters;
    initialize_parameters(&graph_parameters, redis_context);
    
    emit_status(redis_context, NICKNAME, NODE_STARTED, "Consumer initialized and starting");
    
    consume_messages(&graph_parameters);
    
    terminate(&graph_parameters);
    
    return 0;
}
