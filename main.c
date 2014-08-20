#include "rediscounter.h"

int main(int argc, char **argv)
{
    char *usage = "Usage:\nredis-counter -[f rdb file] -[n number] -[a aof filename] -[r]\n"
            "\t-f --file \tspecify full path of rdb file would be parsed.\n"
            "\t-n --number \tspecify number of aof files.\n\t\t\tDefault: 1\n"
            "\t-a --name \tspecify name of aof files. \n\t\t\tDefault: output.aof\n"
            "\t-r --report \tReport mode, don't save aof file. \n\t\t\tDefault: no\n"
            "\t Notice: This tool only test on redis 2.2 and 2.4, so it may be error in 2.4 later.\n";
    if(argc <= 1) {
        fprintf(stderr, usage);
        exit(1);
    }
    char *rdbFile;
    int i;
    for(i = 1; i < argc; i++) {
        if(argc > i+1 && argv[i][0] == '-' && argv[i][1] == 'f') {
            i += 1;
            rdbFile = argv[i];
        }else if(argv[i][0] == '-' && argv[i][1] == 'n'){
            i += 1;
            aof_number = argv[i];
        }
        else if(argv[i][0] == '-' && argv[i][1] == 'a'){
            i += 1;
            aof_filename = argv[i];
        }
        else if(argv[i][0] == '-' && argv[i][1] == 'r'){
            dump_aof = 1;
        }
    }
    if(argc <= 1)    rdbLoad(rdbFile);
    printf("Hello World!\n");
    return 0;
}

