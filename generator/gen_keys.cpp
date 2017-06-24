#include "common.h"
#include "mimir.h"

using namespace MIMIR_NS;

#define WORD_LEN_MEAN_DEFAULT 6
#define WORD_LEN_SD_DEFAULT   1

void parse_cmd_line(int argc, char **argv);

void map (Readable<const char*,void> *input,
          Writable<const char*,void> *output, void *ptr);
void combine (Combinable<const char*,void> *combiner,
              const char**, void*, void*, void*, void *ptr);

const char *cmdstr = "./cmd \t<number of unique words> <outfile> \n\
\t--word-len-mean [val]\n\
\t--word-len-std [val]\n\
\t-sort\n";

uint64_t total_unique = 0;
uint64_t remain_unique = 0;
const char *outfile = NULL;
int len_mean = WORD_LEN_MEAN_DEFAULT;
double len_std = WORD_LEN_SD_DEFAULT;
bool sortkey = false;

int proc_rank, proc_size;

int main(int argc, char **argv) {

    parse_cmd_line(argc, argv);

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &proc_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_size);

    std::random_device rd;
    std::mt19937 gen(rd());

    std::string output = outfile;
    std::vector<std::string> input;

    pid_t pid = getpid();
    fprintf(stdout, "start rank=%d, pid=%ld\n", proc_rank, pid);

    MIMIR_NS::MimirContext<const char*, void>* ctx 
        = new MIMIR_NS::MimirContext<const char*, void>(MPI_COMM_WORLD, map, NULL,
                                        input, output, NULL, combine);

    // Generate remain_unique words
    remain_unique = total_unique;
    while (1) {
        uint64_t nunique = ctx->map();
        remain_unique = total_unique - nunique;
        if (proc_rank == 0) {
            fprintf(stdout, "generate %ld unique words\n",
                    total_unique - remain_unique);
        }
        if (remain_unique == 0) break;
    }

    // Output words to files
    ctx->set_outfile_format("text");
    ctx->output();
    delete ctx;

    if (proc_rank == 0) {
        fprintf(stdout, "done\n");
    }

    return 0;
}

void map (Readable<const char*,void> *input, 
          Writable<const char*,void> *output, void *ptr)
{
    std::vector<std::string> unique_words;

    uint64_t local_unique = remain_unique / proc_size;
    if (proc_rank < (remain_unique % proc_size)) local_unique += 1;

    // Get existing words
    if (input != NULL) {
        const char *exist_word = NULL;
        while (input->read(&exist_word, NULL) == 0) {
            unique_words.push_back(std::string(exist_word));
        }
    }

    // Generate more words
    generate_unique_words(local_unique, unique_words, len_mean, len_std);

    printf("%d[%d] generate unique words success!\n", proc_rank, proc_size);

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

void parse_cmd_line(int argc, char **argv) {

    if (argc < 3) { printf("%s", cmdstr); exit(1); }

    --argc;
    ++argv;
    assert(argc);

    if (**argv == '-') { printf("%s", cmdstr); exit(1); }

    total_unique = atoi(*argv);

    --argc;
    ++argv;
    assert(argc);

    if (**argv == '-') { printf("%s", cmdstr); exit(1); }

    outfile = *argv;

    while (--argc && ++argv) {
        if (!strcmp(*argv, "--word-len-mean")) {
            --argc;
            ++argv;
            assert(argc);

            len_mean = atoi(*argv);
        }
        else if (!strcmp(*argv, "--word-len-std")) {
            --argc;
            ++argv;
            assert(argc);

            len_std = atof(*argv);
        }
        else if (!strcmp(*argv, "-sort")) {
            sortkey = true;
        }
        else {
            printf("%s", cmdstr);
        }
    }
}
