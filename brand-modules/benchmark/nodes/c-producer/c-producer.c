#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>
#include <math.h>
#include <hiredis.h>
#include "brand.h"
#include "nxjson.h"

#define MAX_MESSAGES 100000
#define MAX_OUTPUT_FILE 256

typedef struct {
    int n_messages;
    int freq;
    int msg_size;
    int num_channels;
    char output_file[MAX_OUTPUT_FILE];
} producer_parameters_t;

typedef struct {
    double send_time;
} send_stats_t;

// Global variables for signal handling
redisContext *redis_context = NULL;
send_stats_t send_times[MAX_MESSAGES];
int g_counter = 0;
volatile int flag_SIGINT = 0;

char NICKNAME[20] = "C-producer";
redisReply *reply = NULL;

// Signal handler
void handler_SIGINT(int sig) {
    flag_SIGINT++;
}

// Get current time in seconds (monotonic)
double get_current_time() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

// Produce messages at specified frequency
void produce_messages(producer_parameters_t *params) {
    double period_s = 1.0 / params->freq;
    double next_deadline = get_current_time();
    
    for (int i = 0; i < params->n_messages && !flag_SIGINT; i++) {
        // Busy-wait until next deadline
        while (1) {
            double now = get_current_time();
            if (now >= next_deadline) {
                // Create message with timestamp
                double send_time = now;
                char time_buffer[sizeof(double)];
                memcpy(time_buffer, &send_time, sizeof(double));
                
                // Send to Redis stream
                redisReply *xadd_reply = (redisReply *)redisCommand(redis_context,
                    "XADD random * time %b",
                    time_buffer, sizeof(double));
                
                if (xadd_reply != NULL) {
                    freeReplyObject(xadd_reply);
                } else {
                    fprintf(stderr, "[%s] Error sending to Redis\n", NICKNAME);
                    break;
                }
                
                if (g_counter < MAX_MESSAGES) {
                    send_times[g_counter].send_time = send_time;
                }
                g_counter++;
                next_deadline += period_s;
                break;
            }
            // Sleep for a small amount to avoid busy waiting
            // usleep(1);
        }
    }
}

// Save results to file and print statistics
void terminate(producer_parameters_t *producer_params) {
    printf("Total messages sent: %d\n", g_counter);
    
    // Calculate statistics (excluding first measurement)
    if (g_counter > 1) {
        double sum = 0.0, max_val = 0.0, sum_sq = 0.0;
        int max_idx = 0;
        int count = 0;
        
        for (int i = 1; i < g_counter; i++) {
            double period = send_times[i].send_time - send_times[i-1].send_time;
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
            
            printf("Average send period (s): %.6f\n", mean);
            printf("Max send period (s): %.6f, idx: %d\n", max_val, max_idx);
            printf("Std send period (s): %.6f\n", stddev);
        }
    }
    
    // Save results to file
    if (strlen(producer_params->output_file) > 0) {
        FILE *outfile = fopen(producer_params->output_file, "w");
        if (outfile != NULL) {
            fprintf(outfile, "send_time,send_period\n");
            
            for (int i = 0; i < g_counter; i++) {
                fprintf(outfile, "%.9f,", send_times[i].send_time);
                if (i > 0) {
                    fprintf(outfile, "%.9f", send_times[i].send_time - send_times[i-1].send_time);
                }
                else {
                    fprintf(outfile, "0.0");
                }
                fprintf(outfile, "\n");
            }
            fclose(outfile);
            printf("Results saved to: %s\n", producer_params->output_file);
        } else {
            printf("Error: Could not open output file: %s\n", producer_params->output_file);
        }
    }
    
    printf("Producer stopped.\n");
    
    // Signal completion
    if (redis_context != NULL) {
        redisCommand(redis_context, "XADD test_completion * producer done");
        redisFree(redis_context);
        redis_context = NULL;
    }
}

void initialize_signals() {
    printf("[%s] Attempting to initialize signal handlers.\n", NICKNAME);
    signal(SIGINT, &handler_SIGINT);
    printf("[%s] Signal handlers installed.\n", NICKNAME);
}

void initialize_parameters(producer_parameters_t *p, redisContext *c) {
    char SUPERGRAPH_ID[] = "0";
    const nx_json *supergraph_json = get_supergraph_json(c, reply, SUPERGRAPH_ID);
    
    if (supergraph_json == NULL) {
        emit_status(c, NICKNAME, NODE_FATAL_ERROR, "No supergraph found for initialization. Aborting.");
        exit(1);
    }
    
    // Get parameters from supergraph
    p->n_messages = get_parameter_int(supergraph_json, NICKNAME, "n_messages");
    p->freq = get_parameter_int(supergraph_json, NICKNAME, "freq");
    p->msg_size = get_parameter_int(supergraph_json, NICKNAME, "msg_size");
    p->num_channels = get_parameter_int(supergraph_json, NICKNAME, "num_channels");
    strcpy(p->output_file, get_parameter_string(supergraph_json, NICKNAME, "output_file"));
    
    printf("[%s] Parameters have been loaded.\n", NICKNAME);
    printf("[%s] n_messages: %d\n", NICKNAME, p->n_messages);
    printf("[%s] freq: %d\n", NICKNAME, p->freq);
    printf("[%s] msg_size: %d\n", NICKNAME, p->msg_size);
    printf("[%s] num_channels: %d\n", NICKNAME, p->num_channels);
    printf("[%s] output_file: %s\n", NICKNAME, p->output_file);
    
    if (p->n_messages <= 0 || p->freq <= 0) {
        emit_status(c, NICKNAME, NODE_FATAL_ERROR, "Invalid parameters: n_messages and freq must be positive");
        redisFree(c);
        exit(1);
    }
}

int main(int argc, char **argv) {
    printf("[%s] Parsing command line args...\n", NICKNAME);
    
    // Parse command line args and init redis
    redis_context = parse_command_line_args_init_redis(argc, argv, NICKNAME);
    
    initialize_signals();
    
    // Initialize parameters from supergraph
    producer_parameters_t producer_params;
    initialize_parameters(&producer_params, redis_context);
    
    emit_status(redis_context, NICKNAME, NODE_STARTED, "Producer initialized and starting");
    
    // Run producer
    produce_messages(&producer_params);
    
    // Terminate and save results
    terminate(&producer_params);
    
    return 0;
}
