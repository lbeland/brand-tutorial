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
    int msg_size;
    int num_channels;
    int max_buffer_size;
    char output_file[MAX_OUTPUT_FILE];
} producer_parameters_t;

// Global variables for signal handling
redisContext *redis_context = NULL;
volatile int flag_SIGINT = 0;
double *send_times = NULL;

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

// Busy-wait until there is room in the stream buffer
void wait_for_buffer_slot(int max_buffer_size) {
    while (!flag_SIGINT) {
        redisReply *xlen_reply = (redisReply *)redisCommand(redis_context, "XLEN random");
        if (xlen_reply == NULL) {
            continue;
        }

        int has_slot = 0;
        if (xlen_reply->type == REDIS_REPLY_INTEGER) {
            has_slot = (xlen_reply->integer < max_buffer_size);
        }

        freeReplyObject(xlen_reply);

        if (has_slot) {
            break;
        }
    }
}

// Produce messages as fast as possible
void produce_messages(producer_parameters_t *params) {
    int message_size = params->num_channels * params->msg_size * sizeof(double);
    double *message_data = (double *)malloc(message_size);
    if (!message_data) {
        fprintf(stderr, "[%s] Memory allocation failed\n", NICKNAME);
        return;
    }

    send_times = (double *)malloc(params->n_messages * sizeof(double));
    if (!send_times) {
        fprintf(stderr, "Memory allocation failed\n");
        return;
    }
    
    // Fill with default data (ones)
    for (int i = 0; i < params->num_channels * params->msg_size; i++) {
        message_data[i] = 1.0;
    }
    
    // Warmup loop to stabilize the stream before measurements
    for (int i = 0; i < 500 && !flag_SIGINT; i++) {
        wait_for_buffer_slot(params->max_buffer_size);
        double now = get_current_time();
        // Fill message with current timestamp
        for (int j = 0; j < params->num_channels * params->msg_size; j++) {
            message_data[j] = now;
        }
        
        // Send to Redis stream
        redisReply *xadd_reply = (redisReply *)redisCommand(redis_context,
            "XADD random * time %b",
            message_data, message_size);
        
        if (xadd_reply != NULL) {
            freeReplyObject(xadd_reply);
        } else {
            fprintf(stderr, "[%s] Error sending to Redis\n", NICKNAME);
            break;
        }
    }
    
    // Send measured messages
    double now = 0.0;
    for (int i = 0; i < params->n_messages && !flag_SIGINT; i++) {
        wait_for_buffer_slot(params->max_buffer_size);
        now = get_current_time();
        // Fill message with current timestamp
        for (int j = 0; j < params->num_channels * params->msg_size; j++) {
            message_data[j] = now;
        }
        
        // Send to Redis stream
        redisReply *xadd_reply = (redisReply *)redisCommand(redis_context,
            "XADD random * time %b",
            message_data, message_size);
        
        if (xadd_reply != NULL) {
            freeReplyObject(xadd_reply);
        } else {
            fprintf(stderr, "[%s] Error sending to Redis\n", NICKNAME);
            break;
        }
        
        send_times[i] = now;
    }
    
    free(message_data);
}

// Save results to file and print statistics
void terminate(producer_parameters_t *producer_params) {
    printf("Total messages sent: %d\n", producer_params->n_messages);
    
    // Calculate statistics
    double sum = 0.0, max_val = 0.0, sum_sq = 0.0;
    int max_idx = 0;
    
    for (int i = 1; i < producer_params->n_messages; i++) {
        double period = send_times[i] - send_times[i-1];
        sum += period;
        sum_sq += period * period;
        if (period > max_val) {
            max_val = period;
            max_idx = i;
        }
    }
    
    double mean_sec = sum / producer_params->n_messages;
    double variance = (sum_sq / producer_params->n_messages) - (mean_sec * mean_sec);
    double stddev_sec = sqrt(variance < 0 ? 0 : variance);
    
    // Convert to microseconds
    double mean_us = mean_sec * 1e6;
    double stddev_us = stddev_sec * 1e6;
    double max_us = max_val * 1e6;
    
    printf("Average send period (us): %.4f\n", mean_us);
    printf("Max send period (us): %.4f, idx: %d\n", max_us, max_idx);
    printf("Std send period (us): %.4f\n", stddev_us);
    
    // Save results to CSV file in same format as zeromq
    if (strlen(producer_params->output_file) > 0) {
        FILE *outfile = fopen(producer_params->output_file, "w");
        if (outfile != NULL) {
            fprintf(outfile, "Metric,Send_period\n");
            fprintf(outfile, "mean,%.6f\n", mean_us);
            fprintf(outfile, "std,%.6f\n", stddev_us);
            fprintf(outfile, "max,%.6f\n", max_us);
            fclose(outfile);
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
    signal(SIGINT, &handler_SIGINT);
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
    p->msg_size = get_parameter_int(supergraph_json, NICKNAME, "msg_size");
    p->num_channels = get_parameter_int(supergraph_json, NICKNAME, "num_channels");
    p->max_buffer_size = get_parameter_int(supergraph_json, NICKNAME, "max_buffer_size");
    strcpy(p->output_file, get_parameter_string(supergraph_json, NICKNAME, "output_file"));
    
    if (p->n_messages <= 0) {
        emit_status(c, NICKNAME, NODE_FATAL_ERROR, "Invalid parameters: n_messages must be positive");
        redisFree(c);
        exit(1);
    }
}

int main(int argc, char **argv) {
    
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
