#include "common.h"
#include "mimir.h"

using namespace MIMIR_NS;

#define WORD_LEN_MEAN_DEFAULT 5
#define WORD_LEN_SD_DEFAULT   0
#define VALUE_LEN             4

void parse_cmd_line(int argc, char **argv);

void map_nums (Readable<double, void> *input,
               Writable<double, void> *output, void *ptr);
void map_words (Readable<double, void> *input,
                Writable<const char*, void> *output, void *ptr);
void map_copy (Readable<const char*, void> *input,
               Writable<const char*, void> *output, void *ptr);

void map_uniques (Readable<const char*,void> *input,
                  Writable<const char*,void> *output, void *ptr);

void map_stat (Readable<const char*, uint64_t> *input,
               Writable<const char*, uint64_t> *output, void *ptr);

void combine (Combinable<const char*,void> *combiner,
              const char**, void*, void*, void*, void *ptr);
void scanedge (const char ** word, void *val, void* ptr);

void output_kv(const char **word, void *val, void *ptr);

const char *cmdstr = "./cmd \t<itemcount> <outfile>\n\
\t--zipf-n [val]\n\
\t--zipf-alpha [val]\n\
\t--stat-file [val]\n\
\t--len-mean [val]\n\
\t--len-std [val]\n\
\t--val-len [val]\n\
\t-disorder\n\
\t-exchange\n\
\t-withvalue\n\
\t-distonly\n";

uint64_t itemcount = 0;
const char *outfile = NULL;
//const char *dictfile = 0;
uint64_t zipf_n = 0;
double zipf_alpha = 0.0;
const char *statfile = NULL;
bool disorder = false;
bool idxunique = false;
bool exchange = false;
bool withvalue = false;
bool singlefile = false;
bool distonly = false;

uint64_t total_unique = 0;
uint64_t remain_unique = 0;
int len_mean = WORD_LEN_MEAN_DEFAULT;
double len_std = WORD_LEN_SD_DEFAULT;
int val_len = VALUE_LEN;

int proc_rank, proc_size;
double    *dist_map = NULL;
double    *dist_new_map = NULL;
uint64_t  *div_idx_map = NULL;
double    *div_dist_map = NULL;
uint64_t  *word_counts = NULL;
uint64_t  *div_map = NULL;

std::vector<std::string> unique_words;
std::vector<std::string> unique_new_words;

std::random_device rd;
std::mt19937 gen(rd());
std::uniform_int_distribution<> *dis = NULL;

int parititon_num (double *num, void *null, int npartition) {

    for (int i = 1; i < proc_size + 1; i ++) {
        if (div_dist_map[i] >= *num) return i - 1;
    }

    return proc_size - 1;
}

int partition_word (const char **word, void* null, int npartition) {

    int rank = (*dis)(gen);

    //printf("%d[%d] word=%s, partitioned rank=%d\n",
    //       proc_rank, proc_size, *word, rank);

    return rank;
}

double t_copy = 0.0, t_map = 0.0, t_cvt = 0.0;

int main(int argc, char **argv) {

    parse_cmd_line(argc, argv);

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &proc_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_size);

    dis = new std::uniform_int_distribution<>(0, proc_size - 1);

    double t1 = MPI_Wtime();

    total_unique = zipf_n;
    uint64_t div_size = zipf_n / proc_size;
    if (proc_rank < (zipf_n % proc_size)) div_size += 1;
    dist_map = new double[div_size];
    div_idx_map = new uint64_t[proc_size + 1];
    div_dist_map = new double[proc_size + 1];

    gen_dist_map(zipf_n, zipf_alpha, dist_map, div_idx_map, div_dist_map);

    if (distonly) {
        print_dist_map(zipf_n, zipf_alpha, dist_map);
        delete [] dist_map;
        delete [] div_idx_map;
        delete [] div_dist_map;
        return 0;
    }

    double t2 = MPI_Wtime();
    printf("%d[%d] generate distribution map (walltime=%lf)\n",
           proc_rank, proc_size, t2 - t1);

    div_size = div_idx_map[proc_rank + 1] - div_idx_map[proc_rank];
    dist_new_map = new double[div_size+1];
    word_counts = new uint64_t[div_size];

    for (uint64_t i = 0; i < div_size; i++) {
        word_counts[i] = 0;
    }

    repartition_dist_map(zipf_n, dist_map, div_idx_map, dist_new_map);

    double t3 = MPI_Wtime();
    printf("%d[%d] repartition distribution map (walltime=%lf)\n",
           proc_rank, proc_size, t3 - t2);

    // Generate unique words
    if (!idxunique) {

        MIMIR_NS::MimirContext<const char*, void>* unique_words_ctx 
            = new MIMIR_NS::MimirContext<const char*, void>(std::vector<std::string>(),
                    std::string(), MPI_COMM_WORLD, combine);

        // Generate remain_unique words
        remain_unique = total_unique;
        while (1) {
            uint64_t nunique = unique_words_ctx->map(map_uniques);
            remain_unique = total_unique - nunique;
            printf("%d[%d] generate %ld unique words\n",
                    proc_rank, proc_size, total_unique - remain_unique);
            if (remain_unique == 0) break;
        }

        double t4 = MPI_Wtime();
        printf("%d[%d] generate unique words (walltime=%lf)\n",
                proc_rank, proc_size, t4 - t3);

        MIMIR_NS::MimirContext<const char*, void>* ctx_ptr = NULL;

        if (disorder) {
            MIMIR_NS::MimirContext<const char*, void>* disorder_words_ctx 
                = new MIMIR_NS::MimirContext<const char*, void>(std::vector<std::string>(),
                        std::string(), MPI_COMM_WORLD, NULL, partition_word);
            disorder_words_ctx->insert_data_handle(unique_words_ctx->get_data_handle());
            disorder_words_ctx->map(map_copy);

            delete unique_words_ctx;

            ctx_ptr = disorder_words_ctx;

            double t5 = MPI_Wtime();

            printf("%d[%d] disorder unique words (walltime=%lf)\n",
                    proc_rank, proc_size, t5 - t4);

        } else {
            ctx_ptr = unique_words_ctx;
        }

        double t5 = MPI_Wtime();

        unique_words.clear();
        ctx_ptr->scan(scanedge);
        if (exchange) random_exchange(unique_words);

        repartition_unique_words(unique_words, unique_new_words, div_idx_map);
        unique_words.clear();

        double t6 = MPI_Wtime();
        printf("%d[%d] repartition unique words (walltime=%lf)\n",
                proc_rank, proc_size, t6 - t5);
        fflush(stdout);

        delete ctx_ptr;
    }

    double t6 = MPI_Wtime();

    MIMIR_NS::MimirContext<double, void>* num_ctx 
        = new MIMIR_NS::MimirContext<double, void>(std::vector<std::string>(),
                std::string(), MPI_COMM_WORLD, NULL, parititon_num);

    printf("%d[%d] start generate numbers\n", proc_rank, proc_size);
    fflush(stdout);

    num_ctx->map(map_nums);

    double t7 = MPI_Wtime();
    printf("%d[%d] generate numbers (walltime=%lf)\n",
            proc_rank, proc_size, t7 - t6);

    MIMIR_NS::MimirContext<const char*, void, double, void>* word_ctx 
        = new MIMIR_NS::MimirContext<const char*, void, double, void>(
                std::vector<std::string>(), std::string(),
                MPI_COMM_WORLD, NULL, partition_word);
    word_ctx->insert_data_handle(num_ctx->get_data_handle());

    word_ctx->map(map_words);

    double t8 = MPI_Wtime();
    printf("%d[%d] generate words (walltime=%lf)\n",
            proc_rank, proc_size, t8 - t7);

    delete num_ctx;

    MIMIR_NS::MimirContext<const char*, void>* partition1_ctx 
        = new MIMIR_NS::MimirContext<const char*, void>(
                std::vector<std::string>(), std::string(),
                MPI_COMM_WORLD, NULL, partition_word);
    partition1_ctx->insert_data_handle(word_ctx->get_data_handle());
    partition1_ctx->map(map_copy);

    delete word_ctx;

    MIMIR_NS::MimirContext<const char*, void>* partition2_ctx 
        = new MIMIR_NS::MimirContext<const char*, void>(
                std::vector<std::string>(), std::string(),
                MPI_COMM_WORLD, NULL, partition_word);
    partition2_ctx->insert_data_handle(partition1_ctx->get_data_handle());

    partition2_ctx->map(map_copy);
    char filename[1024];
    sprintf(filename, "%s%d.%d", outfile, proc_size, proc_rank);
    FILE *fp = fopen(filename, "w");
    if (!fp) {
        fprintf(stderr, "Output error!\n");
        exit(1);
    }
    partition2_ctx->scan(output_kv, fp);
    fclose(fp);

    delete partition1_ctx;
    delete partition2_ctx;

    double t9 = MPI_Wtime();

    if (statfile != NULL) {
        char filename[1024];
        sprintf(filename, "%s%d.%d", statfile, proc_size, proc_rank);
#if 0
        std::ofstream outfile;
        outfile.open(filename);
        for (uint64_t i = 0; i < div_idx_map[proc_rank + 1] - div_idx_map[proc_rank] ; i++) {
            if (idxunique) {
                std::string str = get_unique_word(div_idx_map[proc_rank] + i, len_mean);
                outfile << str.c_str() << " " << word_counts[i] << std::endl;
            } else {
                outfile << unique_new_words[i] << " " << word_counts[i] << std::endl;
            }
        }
        outfile.close();
#endif
        MIMIR_NS::MimirContext<const char*, uint64_t>* stat_ctx 
            = new MIMIR_NS::MimirContext<const char*, uint64_t>(
                std::vector<std::string>(), std::string(filename), MPI_COMM_WORLD);
        stat_ctx->map(map_stat, NULL, true, true, "text");
        delete stat_ctx;
    }

    delete [] dist_new_map;
    delete [] div_dist_map;
    delete [] div_idx_map;
    delete [] dist_map;
    delete [] word_counts;

    printf("%d[%d] done t_copy = %lf, t_map = %lf, t_cvt = %lf\n",
           proc_rank, proc_size, t_copy, t_map, t_cvt);
    fflush(stdout);

    delete dis;

    MPI_Finalize();

    return 0;
}

void map_nums (Readable<double,void> *input, 
               Writable<double,void> *output, void *ptr) {
    uint64_t local_items = itemcount / proc_size;
    if (proc_rank < itemcount % proc_size) local_items += 1;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0, 1);
    while (local_items > 0) {
        double num = dis(gen);
        output->write(&num, NULL);
        local_items --;
    }
}

void map_words (Readable<double, void> *input, 
                Writable<const char*, void> *output, void *ptr) {
    double num;
    uint64_t idx;
    const char *word;
    uint64_t range_size = div_idx_map[proc_rank + 1] - div_idx_map[proc_rank];

    while (input->read(&num, NULL) == true) {

        uint64_t low = 0;
        uint64_t high = range_size + 1;
        uint64_t mid = 0;

        double t1 = MPI_Wtime();

        while (low + 1 < high) {
            mid = (low + high) / 2;
            if (num >= dist_new_map[mid] && num < dist_new_map[mid+1])
                break;
            if (num < dist_new_map[mid] && num >= dist_new_map[mid-1]) {
                mid -= 1;
                break;
            }
            if (num < dist_new_map[mid]) high = mid;
            else low = mid;
        }

        double t2 = MPI_Wtime();
        t_cvt += (t2 - t1);

        idx = mid;
        std::string str;
        if (idxunique) {
            str = get_unique_word(div_idx_map[proc_rank] + idx, len_mean);
            word = str.c_str();
        } else {
            word = unique_new_words[idx].c_str();
        }
        word_counts[idx] ++;
        output->write(&word, NULL);

    }
}

void map_copy (Readable<const char*, void> *input,
               Writable<const char*, void> *output, void *ptr)
{
    double t1 = MPI_Wtime();
    const char* word = NULL;
    while (input->read(&word, NULL) == true) {
        output->write(&word, NULL);
    }
    double t2 = MPI_Wtime();
    t_copy += (t2 - t1);
}

void map_uniques (Readable<const char*,void> *input, 
          Writable<const char*,void> *output, void *ptr)
{
    unique_words.clear();

    uint64_t local_unique = remain_unique / proc_size;
    if (proc_rank < (remain_unique % proc_size)) local_unique += 1;

    // Get existing words
    if (input != NULL) {
        const char *exist_word = NULL;
        while (input->read(&exist_word, NULL) == true) {
            //unique_words.push_back(std::string(exist_word));
            output->write(&exist_word, NULL);
        }
    }

    printf("%d start generate unique words\n", proc_rank);

    // Generate more words
    if (local_unique > 1000000) local_unique = 1000000;
    generate_unique_words(local_unique, unique_words, len_mean, len_std);

    printf("%d end generate unique words\n", proc_rank);

    // Add words into Mimir
    const char *word = NULL;
    for (auto iter : unique_words) {
        word = iter.c_str();
        output->write(&word, NULL);
    }
}

void combine (Combinable<const char*,void> *combiner,
              const char **key, void *val1, void *val2,
              void *rval, void *ptr)
{
    return;
}

void scanedge (const char ** word, void *val, void* ptr) {
    unique_words.push_back(*word);
}

void output_kv(const char **word, void *val, void *ptr) {
    FILE *fp = (FILE*)ptr;
    if (withvalue) {
        std::random_device rd;
        std::mt19937 gen(rd());
        int64_t min = pow(10, (val_len - 1));
        int64_t max = pow(10, val_len) - 1;
        std::uniform_int_distribution<> dis(min, max);
        int64_t value = round(dis(gen));
        fprintf(fp, "%s %ld\n", *word, value);
    } else {
        fprintf(fp, "%s\n", *word);
    }
}

void map_stat (Readable<const char*, uint64_t> *input,
               Writable<const char*, uint64_t> *output, void *ptr) {

    const char *key = NULL;
    std::string str;

    for (uint64_t i = 0; i < div_idx_map[proc_rank + 1] - div_idx_map[proc_rank]; i++) {
        if (idxunique) {
            str = get_unique_word(div_idx_map[proc_rank] + i, len_mean);
            key = str.c_str();
        } else {
            key = unique_new_words[i].c_str();
        }
        output->write(&key, &word_counts[i]);
    }
}

void parse_cmd_line(int argc, char **argv) {

    if (argc < 4) { printf("%s", cmdstr); exit(1); }

    --argc;
    ++argv;
    assert(argc);
    if (**argv == '-') { printf("%s", cmdstr); exit(1); }
    itemcount = atoll(*argv);

    --argc;
    ++argv;
    assert(argc);
    if (**argv == '-') { printf("%s", cmdstr); exit(1); }
    outfile = *argv;

    //--argc;
    //++argv;
    //assert(argc);
    //if (**argv == '-') { printf("%s", cmdstr); exit(1); }
    //dictfile = *argv;

    while (--argc && ++argv) {
        if (!strcmp(*argv, "--zipf-n")) {
            --argc;
            ++argv;
            assert(argc);

            zipf_n = atoll(*argv);
        }
        else if (!strcmp(*argv, "--zipf-alpha")) {
            --argc;
            ++argv;
            assert(argc);

            zipf_alpha = atof(*argv);
        }
        else if (!strcmp(*argv, "--stat-file")) {
            --argc;
            ++argv;
            assert(argc);

            statfile = *argv;
        }
        else if (!strcmp(*argv, "--len-mean")) {
            --argc;
            ++argv;
            assert(argc);

            len_mean = atoi(*argv);
        }
        else if (!strcmp(*argv, "--len-std")) {
            --argc;
            ++argv;
            assert(argc);

            len_std = atof(*argv);
        }
        else if (!strcmp(*argv, "--val-len")) {
            --argc;
            ++argv;
            assert(argc);

            val_len = atoi(*argv);
        }
        else if (!strcmp(*argv, "-idxunique")) {
            idxunique = true;
        }
        else if (!strcmp(*argv, "-disorder")) {
            disorder = true;
        }
        else if (!strcmp(*argv, "-exchange")) {
            exchange = true;
        }
        else if (!strcmp(*argv, "-withvalue")) {
            withvalue = true;
        }
        else if (!strcmp(*argv, "-distonly")) {
            distonly = true;
        }
        else if (!strcmp(*argv, "-singlefile")) {
            singlefile = true;
        }
        else {
            printf("%s", cmdstr);
        }
    }
}

