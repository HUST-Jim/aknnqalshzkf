
//  扩充audio至原来的十倍

#include <stdio.h>
#include <stdlib.h>

void printMatrx(char *);

int main(int argc, const char * argv[])
{
    //printMatrx("/Users/zhukaifeng/bishe/datasets/audio.data");
    int header[3] = { 0 };
    FILE* fr = fopen("/home/postgres/datasets/audio.data", "rb");
    if (!fr)
    {
        return -1;
    }
    fread(header, sizeof(int), sizeof(header) / sizeof(int), fr);

    int size_of_float = header[0];
    int n = header[1];
    int d = header[2];
//
//
    float *float_arr = malloc(sizeof(float) * d * n);
        
    // audio.data 是按列存储 fix me
    for (int j = 0; j < d; j++)
    {
        fread(float_arr, sizeof(float), n * d, fr);
    }

    fclose(fr);


    FILE* fw = fopen("/home/postgres/datasets/audio_10.data", "wb");
    header[1] = n * 10;
    fwrite(header, sizeof(int), sizeof(header) / sizeof(int), fw);
    for (int i = 0; i < 10; i++)
        fwrite(float_arr, size_of_float, n * d, fw);
    fclose(fw);
    
    printMatrx("/home/postgres/datasets/audio_10.data");
}


void printMatrx(char *abs_path)
{
    int header[3] = { 0 };
    FILE* fr = fopen(abs_path, "rb");
    
    fread(header, sizeof(int), sizeof(header) / sizeof(int), fr);
        
    int size_of_float = header[0];
    int n = header[1];
    int d = header[2];


    float **matrix = malloc(sizeof(float *) * n);
    for (int i = 0; i < n; i++) {
        matrix[i] = malloc(size_of_float * d);
    }

    // audio.data 是按列存储 fix me
    for (int j = 0; j < d; j++)
    {
        for (int i = 0; i < n; i++)
        {
            fread(&(matrix[i][j]), size_of_float, 1, fr);
        }
    }
    
    printf("file name: %s\n", abs_path);
    printf("n: %d, d: %d, size_of_float: %d\n", n, d, size_of_float);
    for (int i = 0; i < d; i++) {
        printf("%f ,", matrix[0][i]);
    }

    fclose(fr);
}
