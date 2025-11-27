
//updated code
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <math.h>
#include <sys/stat.h>
#include <stdbool.h>
#include <unistd.h>

#define MAX_EVENTS 100000

// Folder Names
#define BI_CALL_DIR "bi_signal_calls"
#define BI_TOWER_DIR "bi_signal_towers"     // NEW: For Tower Bi-Signals
#define MONO_TOWER_DIR "mono_signal_towers" // Renamed for clarity
#define MONO_CALL_DIR "mono_signal_calls"   // Renamed for clarity

// Structure for call data
typedef struct {
    char start_timestamp[30];
    char end_timestamp[30];
    long long calling_party;
    long long called_party;
    int call_duration;
    char disposition[10];
    long long imei;
    int unique_id;
} CallData;

// --- Helper: Format Timestamp for Filenames (YYYY-MM-DD_HH-MM-SS) ---
void format_filename_string(char* raw_ts, char* output_str) {
    if(strlen(raw_ts) < 14) {
        strcpy(output_str, "unknown_time");
        return;
    }
    sprintf(output_str, "%.4s-%.2s-%.2s_%.2s-%.2s-%.2s",
            raw_ts, raw_ts + 4, raw_ts + 6, raw_ts + 8, raw_ts + 10, raw_ts + 12);
}

// --- Random Helpers ---
int get_random(int min, int max) {
    return rand() % (max - min + 1) + min;
}

long long get_random_ll(long long min, long long max) {
    double scale = (double)rand() / RAND_MAX;
    return min + (long long)(scale * (max - min));
}

void get_random_disposition(char *disposition) {
    const char *dispositions[] = {"connected", "busy", "failed", "connected"};
    strcpy(disposition, dispositions[get_random(0, 0)]); // Mostly connected for testing
}

int normal_distribution_call_duration(int threshold) {
    if (threshold <= 0) return 0;
    double mean = threshold / 2.0;
    double stddev = threshold / 4.0;
    double value;
    do {
        double u1, u2;
        do { u1 = ((double)rand() / RAND_MAX); } while (u1 == 0.0);
        u2 = ((double)rand() / RAND_MAX);
        double z = sqrt(-2.0 * log(u1)) * cos(2.0 * M_PI * u2);
        value = mean + z * stddev;
    } while (value < 1 || value > threshold);
    return (int)value;
}

int compare_ints(const void *a, const void *b) {
    return (*(int *)a - *(int *)b);
}

/* Compare CallData records by their end_timestamp for qsort usage */
int compare_by_end_time(const void *a, const void *b) {
    const CallData *ca = (const CallData *)a;
    const CallData *cb = (const CallData *)b;
    return strcmp(ca->end_timestamp, cb->end_timestamp);
}

bool is_unique(int arr[], int size, int num) {
    for (int i = 0; i < size; i++) if (arr[i] == num) return false;
    return true;
}

void add_time(char *start_timestamp, int interval_seconds, char *new_timestamp) {
    int year, month, day, hh, mm, ss, ms;
    sscanf(start_timestamp, "%4d%2d%2d%2d%2d%2d%3d", &year, &month, &day, &hh, &mm, &ss, &ms);

    int total_ms = (hh * 3600 + mm * 60 + ss) * 1000 + ms;
    total_ms += interval_seconds * 1000;

    hh = (total_ms / 3600000) % 24;
    mm = (total_ms / 60000) % 60;
    ss = (total_ms / 1000) % 60;
    ms = total_ms % 1000;

    if (total_ms >= 86400000) { day += 1; hh %= 24; }
    sprintf(new_timestamp, "%04d%02d%02d%02d%02d%02d%03d", year, month, day, hh, mm, ss, ms);
}

// --- 1. BI-SIGNAL GENERATOR (CALLS) ---
// Saves Start Event to one file, End Event to another file (based on their specific times)
void save_bi_signal_call(CallData data) {
    struct stat st = {0};
    if (stat(BI_CALL_DIR, &st) == -1) mkdir(BI_CALL_DIR, 0700);

    char start_fmt[30], end_fmt[30];
    format_filename_string(data.start_timestamp, start_fmt);
    format_filename_string(data.end_timestamp, end_fmt);

    char start_filename[100], end_filename[100];
    // UNIFIED NAMING: "BI_CALL_...csv"
    snprintf(start_filename, sizeof(start_filename), "%s/BI_CALL_%s.csv", BI_CALL_DIR, start_fmt);
    snprintf(end_filename, sizeof(end_filename), "%s/BI_CALL_%s.csv", BI_CALL_DIR, end_fmt);

    FILE *start_file = fopen(start_filename, "a");
    FILE *end_file = fopen(end_filename, "a");

    if (ftell(start_file) == 0) fprintf(start_file, "Unique ID,Event Type,Calling Party,Called Party,Timestamp,Disposition,IMEI\n");
    if (ftell(end_file) == 0)   fprintf(end_file, "Unique ID,Event Type,Calling Party,Called Party,Timestamp,Disposition,IMEI\n");

    // Event Type 0 = START
    fprintf(start_file, "%d,0,%lld,%lld,%s,%s,%lld\n",
            data.unique_id, data.calling_party, data.called_party, data.start_timestamp, data.disposition, data.imei);

    // Event Type 1 = END
    fprintf(end_file, "%d,1,%lld,%lld,%s,%s,%lld\n",
            data.unique_id, data.calling_party, data.called_party, data.end_timestamp, data.disposition, data.imei);

    fclose(start_file);
    fclose(end_file);
}

// --- 2. BI-SIGNAL GENERATOR (TOWERS) --- 
// NEW FEATURE: Splits tower interval into Enter (0) and Exit (1) events
void save_bi_signal_tower(int unique_id, char tower[], char start_ts[], char end_ts[]) {
    struct stat st = {0};
    if (stat(BI_TOWER_DIR, &st) == -1) mkdir(BI_TOWER_DIR, 0700);

    char start_fmt[30], end_fmt[30];
    format_filename_string(start_ts, start_fmt);
    format_filename_string(end_ts, end_fmt);

    char start_filename[100], end_filename[100];
    snprintf(start_filename, sizeof(start_filename), "%s/BI_TOWER_%s.csv", BI_TOWER_DIR, start_fmt);
    snprintf(end_filename, sizeof(end_filename), "%s/BI_TOWER_%s.csv", BI_TOWER_DIR, end_fmt);

    FILE *f1 = fopen(start_filename, "a");
    FILE *f2 = fopen(end_filename, "a");

    if (ftell(f1) == 0) fprintf(f1, "Unique ID,Tower,Event Type,Timestamp\n");
    if (ftell(f2) == 0) fprintf(f2, "Unique ID,Tower,Event Type,Timestamp\n");

    // Event Type 0 = ENTER TOWER
    fprintf(f1, "%d,%s,0,%s\n", unique_id, tower, start_ts);
    // Event Type 1 = EXIT TOWER
    fprintf(f2, "%d,%s,1,%s\n", unique_id, tower, end_ts);

    fclose(f1);
    fclose(f2);
}

// --- 3. MONO-SIGNAL GENERATOR (TOWERS) ---
void save_mono_signal_tower(int unique_id, char tower[], char start_timestamp[], char end_timestamp[]) {
    struct stat st = {0};
    if (stat(MONO_TOWER_DIR, &st) == -1) mkdir(MONO_TOWER_DIR, 0700);

    char readable_ts[30];
    format_filename_string(end_timestamp, readable_ts); // Name file by End Time (completion)

    char filename[100];
    snprintf(filename, sizeof(filename), "%s/MONO_TOWER_%s.csv", MONO_TOWER_DIR, readable_ts);

    FILE *file = fopen(filename, "a");
    if (ftell(file) == 0) fprintf(file, "Unique ID,Tower,Start Timestamp,End Timestamp\n");
    
    fprintf(file, "%d,%s,%s,%s\n", unique_id, tower, start_timestamp, end_timestamp);
    fclose(file);
    
    // GENERATE BI-SIGNAL VERSION HERE
    save_bi_signal_tower(unique_id, tower, start_timestamp, end_timestamp);
}

void generate_tower_data(CallData data) {
    int num_towers = get_random(1, (data.call_duration < 10 ? data.call_duration : 10));
    int intervals[10];

    for (int i = 0; i < num_towers - 1; i++) {
        int rand_num;
        do { rand_num = get_random(1, data.call_duration - 1); } while (!is_unique(intervals, i, rand_num));
        intervals[i] = rand_num;
    }
    qsort(intervals, num_towers - 1, sizeof(int), compare_ints);
    intervals[num_towers - 1] = data.call_duration;

    char cur_start[20];
    strcpy(cur_start, data.start_timestamp);

    for (int i = 0; i < num_towers; i++) {
        char tower[5];
        sprintf(tower, "T%d", get_random(1, 10));
        char end[20];
        add_time(data.start_timestamp, intervals[i], end);

        // Save Mono (Interval)
        save_mono_signal_tower(data.unique_id, tower, cur_start, end);
        strcpy(cur_start, end);
    }
}

// --- 4. MONO-SIGNAL GENERATOR (CALLS) ---
void save_iteration_data(const char *temp_filename, int iteration, int throughput, time_t base_time, int threshold) {
    FILE *file = fopen(temp_filename, "w");
    fprintf(file, "Unique ID,Calling Party,Called Party,Start Time,End Time,Duration (sec),Disposition,IMEI\n");

    CallData records[throughput];
    char end_time_str[30];

    for (int i = 0; i < throughput; i++) {
        CallData data;
        int end_millis = get_random(0, 999);
        struct tm *time_info = localtime(&base_time);

        snprintf(data.end_timestamp, sizeof(data.end_timestamp), "%04d%02d%02d%02d%02d%02d%03d",
                 time_info->tm_year + 1900, time_info->tm_mon + 1, time_info->tm_mday, 
                 time_info->tm_hour, time_info->tm_min, time_info->tm_sec, end_millis);

        if (i == throughput - 1) strcpy(end_time_str, data.end_timestamp);

        get_random_disposition(data.disposition);
        data.call_duration = (strcmp(data.disposition, "busy") == 0) ? 0 : normal_distribution_call_duration(threshold);

        time_t start_time = base_time - data.call_duration;
        struct tm *start_time_info = localtime(&start_time);
        int end2 = (data.call_duration == 0) ? end_millis : get_random(1, end_millis > 0 ? end_millis : 1);

        snprintf(data.start_timestamp, sizeof(data.start_timestamp), "%04d%02d%02d%02d%02d%02d%03d",
                 start_time_info->tm_year + 1900, start_time_info->tm_mon + 1, start_time_info->tm_mday,
                 start_time_info->tm_hour, start_time_info->tm_min, start_time_info->tm_sec, end2);

        // removed this line -- to reduce the number of unique users data.calling_party = 9000000000LL + get_random(0, 999999999);
        // Only 1000 users (High collision/repeat probability)
        data.calling_party = 9000000000LL + get_random(0, 1000);
        data.called_party = 9000000000LL + get_random(0, 999999999);
        data.imei = get_random_ll(100000000000000LL, 999999999999999LL);
        data.unique_id = iteration * 1000 + i + 1;
        records[i] = data;
    }

    qsort(records, throughput, sizeof(CallData), compare_by_end_time);

    for (int i = 0; i < throughput; i++) {
        // make sure your multiplier is always greater than throughput to avoid ID collisions
        records[i].unique_id = iteration * 100000 + i + 1;
        
        // Write Mono-Call (Summary)
        fprintf(file, "%d, %lld, %lld, %s, %s, %d, %s, %lld\n",
                records[i].unique_id, records[i].calling_party, records[i].called_party,
                records[i].start_timestamp, records[i].end_timestamp,
                records[i].call_duration, records[i].disposition, records[i].imei);

        // Generate Bi-Signal Calls
        save_bi_signal_call(records[i]);
        
        // Generate Towers (Mono & Bi inside)
        generate_tower_data(records[i]);
    }
    fclose(file);

    // Rename file to readable format
    char readable_ts[30];
    format_filename_string(end_time_str, readable_ts);
    char final_filename[100];
    snprintf(final_filename, sizeof(final_filename), "%s/MONO_CALL_%s.csv", MONO_CALL_DIR, readable_ts);
    rename(temp_filename, final_filename);
    printf("Generated: %s\n", final_filename);
}

int main() {
    srand(time(NULL));
    int throughput, iterations, threshold;
    printf("Enter throughput: "); scanf("%d", &throughput);
    printf("Enter iterations: "); scanf("%d", &iterations);
    printf("Enter max duration: "); scanf("%d", &threshold);

    if (throughput > MAX_EVENTS) return EXIT_FAILURE;

    struct stat st = {0};
    if (stat(MONO_CALL_DIR, &st) == -1) mkdir(MONO_CALL_DIR, 0700);

    time_t current_time = time(NULL);

    for (int i = 0; i < iterations; i++) {
        char temp_filename[50];
        snprintf(temp_filename, sizeof(temp_filename), "temp_%d.csv", i);
        save_iteration_data(temp_filename, i + 1, throughput, current_time + i, threshold);
        // sleep(1); // Uncomment to simulate real-time speed
    }
    printf("Data generation complete.\n");
    return 0;
}