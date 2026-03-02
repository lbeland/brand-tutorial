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
                                            
                                            // Extract time from the bytes
                                            const char *time_data = entry_dict->element[j + 1]->str;
                                            double send_time = *(double *)time_data;
                                            
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
        // Calculate statistics (excluding first measurement)
        double sum = 0.0, max_val = 0.0, sum_sq = 0.0;
        int max_idx = 0;
        int count = 0;
        
        for (int i = 1; i < g_counter; i++) {
            double period = messages[i].recv_time - messages[i-1].recv_time;
            sum += period;
            sum_sq += period * period;
            if (period > max_val) {
                max_val = period;
                max_idx = i;
            }
            count++;
        }
        
        if (count > 0) {
            double mean = sum / count;
            double variance = (sum_sq / count) - (mean * mean);
            double stddev = sqrt(variance < 0 ? 0 : variance);
            
            printf("Average receive period (s): %.6f\n", mean);
            printf("Max receive period (s): %.6f, idx: %d\n", max_val, max_idx);
            printf("Std receive period (s): %.6f\n", stddev);
        }
    }
    
    // Save results to file
    if (strlen(graph_parameters->output_file) > 0) {
        FILE *outfile = fopen(graph_parameters->output_file, "w");
        if (outfile != NULL) {
            fprintf(outfile, "recv_time,latency,recv_period\n");
            
            for (int i = 0; i < g_counter; i++) {
                fprintf(outfile, "%.9f,%.9f,", messages[i].recv_time, messages[i].latency);
                if (i > 0) {
                    fprintf(outfile, "%.9f", messages[i].recv_time - messages[i-1].recv_time);
                }
                else {
                    fprintf(outfile, "%.9f", 0.0);
                }
                fprintf(outfile, "\n");
            }
            fclose(outfile);
            printf("Results saved to: %s\n", graph_parameters->output_file);
        } else {
            printf("Error: Could not open output file: %s\n", graph_parameters->output_file);
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
