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

#define MAX_MESSAGES 10000

typedef struct {
    double recv_time;
    double latency;
} message_data_t;

int g_counter = 0;
int flag_SIGINT = 0;

message_data_t messages[MAX_MESSAGES];
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

// Main consume loop
void consume_messages(graph_parameters_t *graph_parameters) {
    char last_id[50] = "0-0";
    int n_messages = graph_parameters->n_messages;
    int warmup_counter = 0;
    
    // Warmup phase: receive 500 samples before measurements
    while (warmup_counter < 500 && !flag_SIGINT) {
        // Read from Redis stream
        redisReply *xread_reply = (redisReply *)redisCommand(redis_context, 
            "XREAD COUNT 1 BLOCK 0 STREAMS random %s", last_id);
        
        if (xread_reply != NULL) {
            if (xread_reply->type == REDIS_REPLY_ARRAY && xread_reply->elements > 0) {
                redisReply *stream_data = xread_reply->element[0];
                if (stream_data->elements >= 2) {
                    redisReply *entries = stream_data->element[1];
                    
                    for (size_t i = 0; i < entries->elements; i++) {
                        redisReply *entry = entries->element[i];
                        
                        if (entry->elements >= 2) {
                            // Get entry ID
                            redisReply *entry_id = entry->element[0];
                            if (entry_id->type == REDIS_REPLY_STRING) {
                                strcpy(last_id, entry_id->str);
                            }
                            warmup_counter++;
                        }
                    }
                }
            }
            freeReplyObject(xread_reply);
        }
    }
    
    // Measurement phase
    while (g_counter < n_messages && !flag_SIGINT) {
        double now = get_current_time();
        
        // Read from Redis stream
        redisReply *xread_reply = (redisReply *)redisCommand(redis_context, 
            "XREAD COUNT 1 BLOCK 0 STREAMS random %s", last_id);
        
        if (xread_reply != NULL) {
            if (xread_reply->type == REDIS_REPLY_ARRAY && xread_reply->elements > 0) {
                redisReply *stream_data = xread_reply->element[0];
                if (stream_data->elements >= 2) {
                    redisReply *entries = stream_data->element[1];
                    
                    for (size_t i = 0; i < entries->elements; i++) {
                        redisReply *entry = entries->element[i];
                        
                        if (entry->elements >= 2) {
                            // Get entry ID
                            redisReply *entry_id = entry->element[0];
                            if (entry_id->type == REDIS_REPLY_STRING) {
                                strcpy(last_id, entry_id->str);
                            }
                            
                            // Get entry data
                            redisReply *entry_dict = entry->element[1];
                            if (entry_dict->type == REDIS_REPLY_ARRAY) {
                                // Find the 'time' field
                                for (size_t j = 0; j < entry_dict->elements; j += 2) {
                                    if (entry_dict->element[j]->type == REDIS_REPLY_STRING &&
                                        strcmp(entry_dict->element[j]->str, "time") == 0) {
                                        
                                        if (j + 1 < entry_dict->elements &&
                                            entry_dict->element[j + 1]->type == REDIS_REPLY_STRING) {
                                            
                                            // Extract time from the bytes (first double)
                                            const char *time_data = entry_dict->element[j + 1]->str;
                                            double send_time = *(double *)time_data;
                                            
                                            if (g_counter == 0) {
                                                first_timestamp = send_time;
                                            }
                                            
                                            if (g_counter < MAX_MESSAGES) {
                                                messages[g_counter].recv_time = now;
                                                messages[g_counter].latency = now - send_time;
                                            }
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            freeReplyObject(xread_reply);
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
            double period = messages[i].recv_time - messages[i-1].recv_time;
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
            sum_latency += messages[i].latency;
            sum_sq_latency += messages[i].latency * messages[i].latency;
            if (messages[i].latency > max_latency) {
                max_latency = messages[i].latency;
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
        double throughput = g_counter / (messages[g_counter-1].recv_time - first_timestamp) / 1e3;
        printf("Throughput (msg/ms): %.2f\n", throughput);
        
        // Save results to CSV file in same format as zeromq
        if (strlen(graph_parameters->output_file) > 0) {
            FILE *outfile = fopen(graph_parameters->output_file, "w");
            if (outfile != NULL) {
                fprintf(outfile, "Metric,Recv_period, Latency, Throughput\n");
                fprintf(outfile, "mean,%.6f, %.6f, %.6f\n", avg_period_us, avg_latency_us, throughput);
                fprintf(outfile, "std,%.6f, %.6f, \n", stddev_period_us, stddev_latency_us);
                fprintf(outfile, "max,%.6f, %.6f, \n", max_period_us, max_latency_us);
                fclose(outfile);
                printf("Results saved to: %s\n", graph_parameters->output_file);
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
    printf("[%s] Attempting to initialize signal handlers.\n", NICKNAME);
    signal(SIGINT, &handler_SIGINT);
    printf("[%s] Signal handlers installed.\n", NICKNAME);
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
    
    printf("[%s] Parameters have been loaded.\n", NICKNAME);
    printf("[%s] n_messages: %d\n", NICKNAME, p->n_messages);
    printf("[%s] output_file: %s\n", NICKNAME, p->output_file);
}

// void shutdown_process() {
//     printf("[%s] SIGINT received. Shutting down.\n", NICKNAME);
//     terminate(&g_graph_parameters);
//     exit(0);
// }

int main(int argc, char *argv[]) {
    printf("[%s] Parsing command line args...\n", NICKNAME);
    
    redis_context = parse_command_line_args_init_redis(argc, argv, NICKNAME);
    
    initialize_signals();

    graph_parameters_t graph_parameters;
    initialize_parameters(&graph_parameters, redis_context);
    
    emit_status(redis_context, NICKNAME, NODE_STARTED, "Consumer initialized and starting");
    
    consume_messages(&graph_parameters);
    
    terminate(&graph_parameters);
    
    return 0;
}
