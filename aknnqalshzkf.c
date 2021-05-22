#include "postgres.h"
#include "fmgr.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "def.h"
#include "random.h"
#include "aknnqalshzkf.h"
#include "util.h"
#include "catalog/pg_type.h" // for FLOAT4OID

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>

PG_MODULE_MAGIC;

// -----------------------------------------------------------------------------
//  Constants
// -----------------------------------------------------------------------------
const int   TOPK[]         = { 1, 2, 5, 10, 20, 50, 100 };
const int   MAX_ROUND      = 7;
const int   MAXK           = TOPK[MAX_ROUND - 1];

const float MAXREAL        = 3.402823466e+38F;
const float MINREAL        = -MAXREAL;
const int   MAXINT         = 2147483647;
const int   MININT         = -MAXINT;

const int   SIZEBOOL       = (int) sizeof(bool);
const int   SIZECHAR       = (int) sizeof(char);
const int   SIZEINT        = (int) sizeof(int);
const int   SIZEFLOAT      = (int) sizeof(float);
const int   SIZEDOUBLE     = (int) sizeof(double);

const float E              = 2.7182818F;
const float PI             = 3.141592654F;
const float FLOATZERO      = 1e-6F;
const float ANGLE          = PI / 8.0f;

const int   CANDIDATES     = 100; // fix me



// -----------------------------------------------------------------------------
//  function prototypes
// -----------------------------------------------------------------------------
PG_FUNCTION_INFO_V1(aknnqalsh_index);
PG_FUNCTION_INFO_V1(aknnqalsh_knn);
PG_FUNCTION_INFO_V1(l2_distance);
Datum aknnqalsh_index(PG_FUNCTION_ARGS);
Datum aknnqalsh_knn(PG_FUNCTION_ARGS);
Datum l2_distance(PG_FUNCTION_ARGS);
void fill_data_struct_from_file(char *abs_path, struct Data *data_struct);
void fill_data_table(char *table_name, struct Data data);
void fill_param_table(char *dataset_name, int n, int d, float c, int m, int l, float w);
void build_datahash_i_tables(struct Data data, struct Data hash_functions);

void insert_into_logs(int id, char * log);
void build_datarephash_i_tables(int m, int h);

void read_hashfuncs(int m, int d, float *** hashfuncs);

// Datum
// brute_force(PG_FUNCTION_ARGS)
// {
//     int32 query_id;
//     int32 top_k;

//     // -------------------------------------------------------------------------
//     //  util variables
//     // -------------------------------------------------------------------------
//     StringInfoData query_buf;
//     int ret;
//     int proc;
//     // about get an array from table
//     /* SPI (input tuple) support */
// 	SPITupleTable *tuptable;
// 	HeapTuple	spi_tuple;
// 	TupleDesc	spi_tupdesc;
//     bool isnull;
//     ArrayType *raw_array;
//     float *query_object;
//     float **hashfuncs;
//     int			lenin;
// }

Datum
l2_distance(PG_FUNCTION_ARGS)
{
    ArrayType  *input_array_1;
    ArrayType  *input_array_2;
    float4     res = 0;
    // variables for "deconstructed" array
    Datum      *datums_1;
    Datum      *datums_2;
    bool       *nulls;
    int        count;
    // for loop counter
    int        i;
    input_array_1 = PG_GETARG_ARRAYTYPE_P(0);
    input_array_2 = PG_GETARG_ARRAYTYPE_P(1);
    // check that we do indeed have a one-dimensional int array Assert(ARR_ELEMTYPE(input_array) == INT4OID);
    if (ARR_NDIM(input_array_1) > 1) ereport(ERROR,
                (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                 errmsg("1-dimensional array needed")));
    if (ARR_NDIM(input_array_2) > 1) ereport(ERROR,
                (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                 errmsg("1-dimensional array needed")));
    
    deconstruct_array(input_array_1, // one-dimensional array FLOAT4OID
                      FLOAT4OID,
                      4,            // size of float4 in bytes
                      true,         // int4 is pass-by value
                      'i',          // alignment type is 'i'
                      &datums_1, &nulls, &count); // result here
    deconstruct_array(input_array_2,
                      FLOAT4OID,
                      4,            
                      true,         
                      'i',          
                      &datums_2, &nulls, &count);
    for(i = 0; i < count; i++)
    {
        //elog(INFO, "%f", DatumGetFloat4(datums_1[i]));
        //elog(INFO, "%f", DatumGetFloat4(datums_2[i]));
        res += (DatumGetFloat4(datums_1[i]) - DatumGetFloat4(datums_2[i])) * (DatumGetFloat4(datums_1[i]) - DatumGetFloat4(datums_2[i]));
    }

    
    res = sqrt(res);
    //elog(INFO, "res = %f", res);
    PG_RETURN_FLOAT4(res);

    
//     ArrayType   *input_array_1 = PG_GETARG_ARRAYTYPE_P(0);
//     ArrayType   *input_array_2 = PG_GETARG_ARRAYTYPE_P(1);
//     float       res = -1;
//     /* 特别需要注意，pg 内部返回的浮点数组默认元素为 float8 型 */
//     float8      *p_1 = ( (float8 *) ARR_DATA_PTR(input_array_1) );
//     float8      *p_2 = ( (float8 *) ARR_DATA_PTR(input_array_2) );
//     int         lenin = ArrayGetNItems(ARR_NDIM(input_array_1), ARR_DIMS(input_array_1));
    
//     elog(INFO, "lenin = %d", lenin);

//     float       *p1 = palloc(SIZEFLOAT * lenin);
//     float       *p2 = palloc(SIZEFLOAT * lenin);
    
//     for (int i = 0; i < lenin; i++)
//     {
//         elog(INFO, "p_1[%d] = %f", i, p_1[i]);
//         p1[i] = (float) p_1[i];
//         elog(INFO, "p_2[%d] = %f", i, p_2[i]);
//         p2[i] = (float) p_2[i];
//     }
    
//     res = calc_l2_dist(lenin, p1, p2);
//     elog(INFO, "res = %f", res);
//     elog(INFO, "size of res = %lu", sizeof(res));
// //   res = 0;
// //     //pfree(p1);
// //     //pfree(p2);
//     PG_RETURN_FLOAT4(res);
}


/* 
数据集文件路径、查询集文件路径、 数据集名称、n、d、c、h
void
select aknnqalsh_index('/home/postgres/datasets/audio.data','/home/postgres/datasets/audio.data','audio', 54387, 192, 2, 200);
*/
Datum
aknnqalsh_index(PG_FUNCTION_ARGS)
{
    // -------------------------------------------------------------------------
    //  aknnqalsh_index function arguments
    // -------------------------------------------------------------------------
    char  *abs_path_data;
    char  *abs_path_query;
    char  *dataset_name;
    int32  n;
    int32  d;
    float4 c;
    int32  h;
    
    // -------------------------------------------------------------------------
    //  util variables
    // -------------------------------------------------------------------------
    struct Data data_;
    struct Data query_;
    struct Data hash_functions_;
    char *command_create_table_data_query_param;
    
    // -------------------------------------------------------------------------
	//  parameters for QALSH
	// -------------------------------------------------------------------------
	int n_pts_;
	int dim_;
	int ratio_; // approximation ratio

    // -------------------------------------------------------------------------
    //  init aknnqalsh_index function arguments
    // -------------------------------------------------------------------------
    abs_path_data  = text_to_cstring(PG_GETARG_TEXT_PP(0));
    abs_path_query = text_to_cstring(PG_GETARG_TEXT_PP(1));
    dataset_name   = text_to_cstring(PG_GETARG_TEXT_PP(2));
    n = PG_GETARG_INT32(3);
    d = PG_GETARG_INT32(4);
    c = PG_GETARG_FLOAT4(5);
    h = PG_GETARG_INT32(6);

    // -------------------------------------------------------------------------
    //  create talbes: data, query, param, hashfuncs, exe_logs
    // -------------------------------------------------------------------------
    command_create_table_data_query_param = 
    "DROP TABLE IF EXISTS data;\
    DROP TABLE IF EXISTS query;\
    DROP TABLE IF EXISTS param;\
    DROP TABLE IF EXISTS hashfuncs;\
    DROP TABLE IF EXISTS results;\
    CREATE TABLE data (id int, coordinate real[]);\
    CREATE TABLE query (id int, coordinate real[]);\
    CREATE TABLE param (datasetname text, n int, d int, c real, m int, l int, w real);\
    CREATE TABLE hashfuncs (id int, hash_func real[]);\
    CREATE TABLE results (id int, distance real)";
    ereport(INFO,
					(errmsg("creating tables: data, query, param, hashfuncs")));
    SPI_connect();
    SPI_exec(command_create_table_data_query_param, 0);
    //SPI_finish();
    //insert_into_logs(1, "create tables");

    // -------------------------------------------------------------------------
    //  fill the struct Data data_ and data table
    // -------------------------------------------------------------------------
    fill_data_struct_from_file(abs_path_data, &data_);
    fill_data_table("data", data_);
    SPI_exec("CREATE INDEX data_id ON data USING hash (id);", 0);
    
    // -------------------------------------------------------------------------
    //  fill the struct Data query_ and query table
    // -------------------------------------------------------------------------
    fill_data_struct_from_file(abs_path_query, &query_);
    fill_data_table("query", query_);
    SPI_exec("CREATE INDEX query_id ON query USING hash (id);", 0);
    // -------------------------------------------------------------------------
	//  init parameters for QALSH
	// -------------------------------------------------------------------------
	n_pts_ = n;
	dim_   = d;
	ratio_ = c; // approximation ratio

	// -------------------------------------------------------------------------
	//  init <w_> <m_> and <l_> (auto tuning-w)
	//  
	//  w0 ----- best w for L_{0.5} norm to minimize m (auto tuning-w)
	//  w1 ----- best w for L_{1.0} norm to minimize m (auto tuning-w)
	//  w2 ----- best w for L_{2.0} norm to minimize m (auto tuning-w)
	//  other w: use linear combination for interpolation
	// -------------------------------------------------------------------------
	float delta = 1.0f / E;
	float beta  = (float) CANDIDATES / (float) n_pts_;
	float w2 = sqrt((8.0f * SQR(ratio_) * log(ratio_)) / (SQR(ratio_) - 1.0f));
	float p1 = -1.0f, p2 = -1.0f;

	int w_ = w2; // fix me
	p1 = calc_l2_prob(w_ / 2.0f);
	p2 = calc_l2_prob(w_ / (2.0f * ratio_));

	float para1 = sqrt(log(2.0f / beta));
	float para2 = sqrt(log(1.0f / delta));
	float para3 = 2.0f * (p1 - p2) * (p1 - p2);
	float eta   = para1 / para2;
	float alpha = (eta * p1 + p2) / (1.0f + eta);

	int m_ = (int) ceil( (para1 + para2) * (para1 + para2) / para3 );
	int l_ = (int) ceil(alpha * m_);

	// -------------------------------------------------------------------------
	//  generate hash functions
	// -------------------------------------------------------------------------
    hash_functions_.n = m_;
    hash_functions_.d = dim_;
    hash_functions_.matrix = palloc(sizeof(float*) * m_);
	for (int i = 0; i < m_; ++i) {
        hash_functions_.matrix[i] = palloc(sizeof(float) * dim_);
		for (int j = 0; j < dim_; ++j) {
            hash_functions_.matrix[i][j] = gaussian(0.0f, 1.0f);
		}
	}

	// -------------------------------------------------------------------------
	//  fill the table param
	// -------------------------------------------------------------------------
    fill_param_table(dataset_name, n, d, c, m_, l_, w_);
    
    // -------------------------------------------------------------------------
	//  fill the table hashfuncs
	// -------------------------------------------------------------------------
    fill_data_table("hashfuncs", hash_functions_);
    
    // -------------------------------------------------------------------------
	//  build datahash_i and datarephash_i tables, i = [1: m]
	// -------------------------------------------------------------------------
    build_datahash_i_tables(data_, hash_functions_);
    build_datarephash_i_tables(m_, h);
    
    
    pfree(abs_path_data);
    pfree(abs_path_query);
    pfree(dataset_name);

    SPI_finish();
    PG_RETURN_INT32(n);
}

void
build_datahash_i_tables(struct Data data, struct Data hash_functions)
{
    char command[1000];
    int i, j;
    float hash;
    //SPI_connect();
    for (i = 0; i < hash_functions.n; i++)
    {
        sprintf(command, "DROP TABLE IF EXISTS datahash_%d; CREATE TABLE datahash_%d (id int, hash float);", i + 1, i + 1);
        SPI_exec(command, 0);
        // building datahash_i table
        for (j = 0; j < data.n; j++)
        {
            hash = calc_inner_product(hash_functions.d, hash_functions.matrix[i], data.matrix[j]);
            sprintf(command, "INSERT INTO datahash_%d VALUES (%d, %f)", i + 1, j + 1, hash);
            SPI_exec(command, 0);
        }
    }
    //SPI_finish();
}

void 
build_datarephash_i_tables(int m, int h) // h: 每组聚合的向量个数
{
    //ereport(INFO,
	//				(errmsg("building table: datarephash_%d")));
    char command[1000];
    int i;
    //SPI_connect();
    for (i = 0; i < m; i++)
    {
        sprintf(command, "DROP TABLE IF EXISTS datarephash_%d; CREATE TABLE datarephash_%d (id int, rephash real, idarray int[])", i + 1, i + 1);
        SPI_exec(command, 0);
        sprintf(command, "INSERT INTO datarephash_%d SELECT (n-1)/%d+1 AS id, min(x.hash) AS rephash, array_agg(x.id) AS idarray FROM (SELECT id, hash, row_number() OVER (order by hash) AS n FROM datahash_%d) x(id, hash, n) GROUP BY (n-1)/%d ORDER BY id;", 
        i + 1, h, i + 1, h);
        SPI_exec(command, 0);

        // add b+ tree index
        sprintf(command, "CREATE INDEX ix_rephash_%d ON datarephash_%d USING btree (rephash);", i + 1, i + 1);
        SPI_exec(command, 0);
    }
    //SPI_finish();
}

// -------------------------------------------------------------------------
//  fill a matrix from a disk file
// -------------------------------------------------------------------------
void 
fill_data_struct_from_file(char *abs_path, struct Data *data_struct)
{
    int header[3] = { 0 };
    FILE* fr = fopen(abs_path, "rb");
    if (!fr)
    {
        ereport(ERROR,
            (errcode(ERRCODE_IO_ERROR),
             errmsg("failed to open file %s\n", abs_path)));
    }
    fread(header, sizeof(int), sizeof(header) / sizeof(int), fr);
    
    data_struct->size_of_float = header[0];
    data_struct->n = header[1];
    data_struct->d = header[2];

    data_struct->matrix = palloc(sizeof(float *) * data_struct->n);
    for (int i = 0; i < data_struct->n; i++)
    {
        (data_struct->matrix)[i] = palloc(sizeof(float) * data_struct->d);
    }
    
    // audio.data 是按列存储 fix me
    for (int j = 0; j < data_struct->d; j++)
    {
        for (int i = 0; i < data_struct->n; i++)
        {
            fread(&( (data_struct->matrix)[i][j] ), data_struct->size_of_float, 1, fr);
        }
    }
    
    fclose(fr);
}

// -------------------------------------------------------------------------
//  fill the param table
// -------------------------------------------------------------------------
void 
fill_param_table(char *dataset_name, int n, int d, float c, int m, int l, float w)
{
    ereport(INFO,
					(errmsg("filling table: param")));
    char command_insert[1000];
    //SPI_connect();
    sprintf(command_insert, "INSERT INTO param VALUES ('%s', %d, %d, %f, %d, %d, %f)", dataset_name, n, d, c, m, l, w);
    SPI_exec(command_insert, 0);
    //SPI_finish();
}

// -------------------------------------------------------------------------
//  fill a data table from a struct Data
// -------------------------------------------------------------------------
void
fill_data_table(char *table_name, struct Data data)
{
    ereport(INFO,
					(errmsg("filling table: \"%s\"", table_name)));
    char one_vector_str[10000] = "["; // fix me
    char one_float_str[100]; // fix me
    int i, j;
    float number;
    char command_insert[1000];
    
    //SPI_connect();
    for (i = 0; i < data.n; i++)
    {
        one_vector_str[0] = '[';
        one_vector_str[1] = '\0';
        for (j = 0; j < data.d - 1; j++)
        {
            number = data.matrix[i][j];
            sprintf(one_float_str, "%f", number);
            strcat(one_vector_str, one_float_str);
            strcat(one_vector_str, ",");
        }
        number = data.matrix[i][j];
        sprintf(one_float_str, "%f", number);
        strcat(one_vector_str, one_float_str);
        strcat(one_vector_str, "]");

        sprintf(command_insert, "INSERT INTO %s VALUES (%d, Array %s)", table_name, i + 1, one_vector_str);
        // caution: the id of data table begin at 1, so use i + 1
        SPI_exec(command_insert, 0);
    }
    
    //SPI_finish();
}



void
insert_into_logs(int id, char * log) 
{   
    //SPI_connect();
    char command[300];
    sprintf(command, "INSERT INTO exe_logs VALUES (%d, '%s')", id, log);
    SPI_exec(command, 0);
    //SPI_finish();
}

// -------------------------------------------------------------------------
//  Arguments: id of a query vector, k (top k vectors)
//  Usage: SELECT aknnqalsh_knn(1, 1);
// -------------------------------------------------------------------------
Datum
aknnqalsh_knn(PG_FUNCTION_ARGS)
{
    // -------------------------------------------------------------------------
    //  aknnqalsh_knn function arguments
    // -------------------------------------------------------------------------
    int32 query_id;
    int32 top_k;

    // -------------------------------------------------------------------------
    //  util variables
    // -------------------------------------------------------------------------
    StringInfoData query_buf;
    int ret;
    int proc;
    // about get an array from table
    /* SPI (input tuple) support */
	SPITupleTable *tuptable;
	HeapTuple	spi_tuple;
	TupleDesc	spi_tupdesc;
    bool isnull;
    ArrayType *raw_array;
    float *query_object;
    float **hashfuncs;
    int			lenin;

    float *query_object_hashes;
    float query_object_hash;
    int *frequency;
    int *curr_idarray;

    // -------------------------------------------------------------------------
    //  init aknnqalsh_knn function arguments
    // -------------------------------------------------------------------------
    query_id = PG_GETARG_INT32(0);
    top_k = PG_GETARG_INT32(1);
    
    // -------------------------------------------------------------------------
    //  建立 result 表
    // -------------------------------------------------------------------------
    /* spi connect */
    // if ((ret = SPI_connect()) < 0)
	// 	elog(ERROR, "aknnqalsh_knn: SPI_connect returned %d", ret);

    // /* init query buffer */
	// initStringInfo(&query_buf);
    
    // /* create result table */
    // appendStringInfo(&query_buf, "CREATE TABLE result (id int, data_id int, )");
    
    // if ((ret = SPI_exec(query_buf.data, 0)) != SPI_OK_SELECT) 
    //     elog(ERROR, "aknnqalsh_knn: SPI execution failed for query %s", query_buf.data);
    // resetStringInfo(&query_buf);

    // proc = SPI_processed;

    // -------------------------------------------------------------------------
    //  查询数据表query 获得查询对象的coordinate 保存在一个array中 也保存在一个string中 用于后续查询sql的拼接
    // -------------------------------------------------------------------------

    /* spi connect */
    if ((ret = SPI_connect()) < 0)
		elog(ERROR, "aknnqalsh_knn: SPI_connect returned %d", ret);

    /* init query buffer */
	initStringInfo(&query_buf);
    
    /* 清空一下 results 表 */
    appendStringInfo(&query_buf, "TRUNCATE results");
    SPI_exec(query_buf.data, 0);
    resetStringInfo(&query_buf);

    /* 从 query 表中查出 query 对象的 coordinate */
    appendStringInfo(&query_buf, "SELECT coordinate FROM query WHERE id = %d", query_id);
    
    if ((ret = SPI_exec(query_buf.data, 0)) != SPI_OK_SELECT)
		elog(ERROR, "aknnqalsh_knn: SPI execution failed for query %s",
			 query_buf.data);
    resetStringInfo(&query_buf);

    proc = SPI_processed;

    if (proc != 1)
    {
        elog(ERROR, "aknnqalsh_knn: 给定 id=%d 对应的查询对象个数不为 1, 请检查 id 是否正确", query_id);
    }

	tuptable = SPI_tuptable;
    spi_tuple = tuptable->vals[0];
	spi_tupdesc = tuptable->tupdesc;
    
    raw_array = DatumGetArrayTypeP(SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull));

    lenin = ArrayGetNItems(ARR_NDIM(raw_array), ARR_DIMS(raw_array));
    query_object = ( (float *) ARR_DATA_PTR(raw_array) );

    //  for (int i = 0; i < lenin; i++)
    //      elog(INFO, "%f,", query_coordinate[i]);


    // ---------------------------------------------------------------------
	//  从 param 表中把参数读出来
	// ---------------------------------------------------------------------

    /* get datasetname */
	appendStringInfo(&query_buf, "SELECT datasetname FROM param");
	if ((ret = SPI_exec(query_buf.data, 0)) != SPI_OK_SELECT)
		elog(ERROR, "aknnqalsh_knn: SPI execution failed for query %s",
			 query_buf.data);
    resetStringInfo(&query_buf);
	proc = SPI_processed;
	tuptable = SPI_tuptable;
    spi_tuple = tuptable->vals[0];
	spi_tupdesc = tuptable->tupdesc;
    char *datasetname = SPI_getvalue(spi_tuple, spi_tupdesc, 1);
    //elog(INFO, "datasetname: %s", datasetname);
    
    /* get n */
    appendStringInfo(&query_buf, "SELECT n FROM param");
	if ((ret = SPI_exec(query_buf.data, 0)) != SPI_OK_SELECT)
		elog(ERROR, "aknnqalsh_knn: SPI execution failed for query %s",
			 query_buf.data);
    resetStringInfo(&query_buf);
	proc = SPI_processed;
	tuptable = SPI_tuptable;
    spi_tuple = tuptable->vals[0];
	spi_tupdesc = tuptable->tupdesc;
    int n = DatumGetInt32(SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull));
    //elog(INFO, "n(n_pts_): %d", n);

    /* get d */
    appendStringInfo(&query_buf, "SELECT d FROM param");
	if ((ret = SPI_exec(query_buf.data, 0)) != SPI_OK_SELECT)
		elog(ERROR, "aknnqalsh_knn: SPI execution failed for query %s",
			 query_buf.data);
    resetStringInfo(&query_buf);
	proc = SPI_processed;
	tuptable = SPI_tuptable;
    spi_tuple = tuptable->vals[0];
	spi_tupdesc = tuptable->tupdesc;
    int d = DatumGetInt32(SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull));
    //elog(INFO, "d(dim_): %d", d);

    /* get c */
    appendStringInfo(&query_buf, "SELECT c FROM param");
	if ((ret = SPI_exec(query_buf.data, 0)) != SPI_OK_SELECT)
		elog(ERROR, "aknnqalsh_knn: SPI execution failed for query %s", query_buf.data);
    resetStringInfo(&query_buf);
	proc = SPI_processed;
	tuptable = SPI_tuptable;
    spi_tuple = tuptable->vals[0];
	spi_tupdesc = tuptable->tupdesc;
    float c = DatumGetFloat4(SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull));
    //elog(INFO, "c(ratio_): %f", c);

    /* get m */
    appendStringInfo(&query_buf, "SELECT m FROM param");
	if ((ret = SPI_exec(query_buf.data, 0)) != SPI_OK_SELECT)
		elog(ERROR, "aknnqalsh_knn: SPI execution failed for query %s",
			 query_buf.data);
    resetStringInfo(&query_buf);
	proc = SPI_processed;
	tuptable = SPI_tuptable;
    spi_tuple = tuptable->vals[0];
	spi_tupdesc = tuptable->tupdesc;
    int m = DatumGetInt32(SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull));
    //elog(INFO, "m(m_): %d", m);

    /* get l */
    appendStringInfo(&query_buf, "SELECT l FROM param");
	if ((ret = SPI_exec(query_buf.data, 0)) != SPI_OK_SELECT)
		elog(ERROR, "aknnqalsh_knn: SPI execution failed for query %s", query_buf.data);
    resetStringInfo(&query_buf);
	proc = SPI_processed;
	tuptable = SPI_tuptable;
    spi_tuple = tuptable->vals[0];
	spi_tupdesc = tuptable->tupdesc;
    int l = DatumGetInt32(SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull));
    //elog(INFO, "l(l_): %d", l);

    /* get w */
    appendStringInfo(&query_buf, "SELECT w FROM param");
	if ((ret = SPI_exec(query_buf.data, 0)) != SPI_OK_SELECT)
		elog(ERROR, "aknnqalsh_knn: SPI execution failed for query %s", query_buf.data);
    resetStringInfo(&query_buf);
	proc = SPI_processed;
	tuptable = SPI_tuptable;
    spi_tuple = tuptable->vals[0];
	spi_tupdesc = tuptable->tupdesc;
    float w = DatumGetFloat4(SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull));
    //elog(INFO, "w(w_): %f", w);

    // -------------------------------------------------------------------------
    //  做 range search， 找到 k 个最近邻
    // -------------------------------------------------------------------------
    hashfuncs = palloc(sizeof(float *) * m);
    for (int i = 0; i < m; i++)
        hashfuncs[i] = palloc(sizeof(float) * d);
    read_hashfuncs(m, d, &hashfuncs);
    //elog(INFO, "======================== now printing the first hash fucntion ====================");
    // for (int index_1 = 0; index_1 < m; index_1++)
    // {
    //     elog(INFO, "%f ", hashfuncs[0][index_1]);
    // }
    //elog(INFO, "%f ", hashfuncs[0][0]);
    //elog(INFO, "%f ", hashfuncs[0][d-1]);
    //elog(INFO, "======================== end printing the first hash fucntion ====================");
    // 以上获取了 所有的 hash 函数

    // -----------------------------------------------------------------------------------------
    //  计算 对象 的 m 个 hash 值
    query_object_hashes = palloc(sizeof(float) * m);
    for (int i = 0; i < m; i++)
        query_object_hashes[i] = calc_inner_product(d, hashfuncs[i], query_object);
    //elog(INFO, "======================== 开始：打印查询对象的 hash 值 ====================");
    // for (int index_1 = 0; index_1 < m; index_1++)
    // {
    //     elog(INFO, "%f ", query_object_hashes[index_1]);
    // }
    //elog(INFO, "%f ", query_object_hashes[0]);
    //elog(INFO, "%f ", query_object_hashes[m-1]);
    //elog(INFO, "======================== 结束：打印查询对象的 hash 值 ====================");
    // // -----------------------------------------------------------------------------------------

    frequency = palloc(sizeof(int) * n);
    memset(frequency, 0, sizeof(int) * n);
    float radius = init_radius(query_object_hashes, m, c, w);
    float old_radius = 0;
    //elog(INFO, "==========================================================");
    //elog(INFO, "====================init radius is %f=============================", radius);
    //elog(INFO, "==========================================================");
    int   max_num_of_candidates = CANDIDATES + top_k - 1;
    int   *candidate_list = palloc(SIZEINT * max_num_of_candidates);      // 用来存 频繁碰撞对象 的 id
    int   num_of_candidates = 0;
    float *candidate_distance_list = palloc(SIZEFLOAT * max_num_of_candidates);
    
    int   *result_list = palloc(SIZEINT * top_k);         // 用来存 满足|o - q| <= R * c 对象 的 id
    int   num_of_results = 0;
    float *result_distance_list = palloc(SIZEFLOAT * top_k);
    
    bool  flag = false;

    while (true)
    {

        //elog(INFO, "==================== 本轮 radius: %f ========================", radius);
        
        for (int hash_func_id = 1; hash_func_id <= m; hash_func_id++)
        {
        // 对于每一张 hash 表

            query_object_hash = query_object_hashes[hash_func_id - 1];
            // ----------------------------------------------------------------------------
            //  查询数据表 datarephash_i 找到在 范围 中的那些 id
            // ----------------------------------------------------------------------------
            appendStringInfo(&query_buf, 
                "SELECT idarray FROM datarephash_%d WHERE (rephash > %f AND rephash <= %f) OR (rephash >= %f AND rephash < %f)", 
                hash_func_id, 
                query_object_hash - (w * radius / 2.0f), 
                query_object_hash - (w * old_radius / 2.0f), 
                query_object_hash + (w * radius / 2.0f),
                query_object_hash + (w * old_radius / 2.0f)
                );
	        if ((ret = SPI_exec(query_buf.data, 0)) != SPI_OK_SELECT)
		        elog(ERROR, "aknnqalsh_knn: SPI execution failed for query %s", query_buf.data);
            resetStringInfo(&query_buf);
            
            proc = SPI_processed;
            //elog(INFO, "第%d个哈希表，返回 idarray 结果个数是 %d", hash_func_id, proc);
	        tuptable = SPI_tuptable;
            
            if (proc <= 0)
                continue; // 在这条线上，在当前半径下，找不到落在球内的点
            
            for (int tuple_index = 0; tuple_index < proc; tuple_index++)
            {
                // 对于返回的表的每一行
                spi_tuple = tuptable->vals[tuple_index];
                spi_tupdesc = tuptable->tupdesc;
        
                raw_array = DatumGetArrayTypeP(SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull));
                //char * raw_array_str = SPI_getvalue(spi_tuple, spi_tupdesc, 1);
                //elog(INFO, raw_array_str);
                lenin = ArrayGetNItems(ARR_NDIM(raw_array), ARR_DIMS(raw_array));
                //elog(INFO, "lenin : %d", lenin);
                curr_idarray = ( (int *) ARR_DATA_PTR(raw_array) );
                
                for (int j = 0; j < lenin; j++)
                {
                // 遍历当前 idarray，对其中每一个频繁碰撞对象，查出其 coordinate，看是否符合距离 query 足够近
                    int curr_id = curr_idarray[j];
                    frequency[curr_id] += 1;
                    
                    if (frequency[curr_id] > l)
                    {
                    // 是频繁碰撞对象
                        // 先看是否在 result 集里
                        int num_in_result_list_index = num_in_array(curr_id, result_list, num_of_results);
                        if (num_in_result_list_index != -1)
                        {
                            // 这个碰撞频繁对象已经在 result 里了，无需再处理它
                            continue;
                        }

                        int num_in_candidate_list_index = num_in_array(curr_id, candidate_list, num_of_candidates);
                        if (num_in_candidate_list_index != -1)
                        {
                            // 这个碰撞频繁对象已经在 candidate 里了，无需再处理它
                            continue;
                        }

                        //elog(INFO, "当前向量 %d 出现次数大于 l=%d 次", curr_id,l);
                        
                        appendStringInfo(&query_buf, 
                                "SELECT coordinate FROM data WHERE id = %d", 
                                curr_id);
                        if ((ret = SPI_exec(query_buf.data, 0)) != SPI_OK_SELECT)
                            elog(ERROR, "aknnqalsh_knn: SPI execution failed for query %s", query_buf.data);
                        // 因为 data 的 id 列加了 hash 索引，所以这一步查询很快

                        proc = SPI_processed;

                        if (proc != 1)
                            elog(ERROR, "aknnqalsh_knn: SPI execution failed, 在表 data中 找 coordinate %s", query_buf.data);
        
                        resetStringInfo(&query_buf);

                        tuptable = SPI_tuptable;
                        spi_tuple = tuptable->vals[0];
                        spi_tupdesc = tuptable->tupdesc;
        
                        raw_array = DatumGetArrayTypeP(SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull));
                        float *possible_good = ( (float *) ARR_DATA_PTR(raw_array) );
                        float tmp_dis = calc_l2_dist(d, query_object, possible_good);

                        // 当前对象加入到 candidate 集中
                        candidate_list[num_of_candidates] = curr_id;
                        candidate_distance_list[num_of_candidates] = tmp_dis;
                        num_of_candidates++;
                        
                        if ( num_of_candidates >= max_num_of_candidates )
                        {
                            flag = true;
                            break;
                        }

                        if (tmp_dis <= c * radius)
                        {// 当前对象满足 |o - q| <= c * R
                            // 当前对象加入到 result 集中
                            result_list[num_of_results] = curr_id;
                            result_distance_list[num_of_results] = tmp_dis;
                            num_of_results++;
                            
                            if ( num_of_results >= top_k )
                            {
                                flag = true;
                                break;
                            }
                        }
                    }
                }
                if (flag) break;
            }
            if (flag) break;
        }
        if (flag) break;
        
        old_radius = radius;
        radius = update_radius(radius, query_object_hashes, m, c, w);
    }

    

    // -------------------------------------------------------------------------
    //  将结果插入到 results 表
    // -------------------------------------------------------------------------
    if (num_of_results >= top_k)
    {
        //elog(INFO, "-------------------------------------------------------------------------");
        //elog(INFO, "result_list 满了");
        //elog(INFO, "-------------------------------------------------------------------------");
        // 通过 result_list, result_distance_list 构建结果
        for (int i = 0; i < num_of_results; i++)
        {
            resetStringInfo(&query_buf);
            appendStringInfo(&query_buf, "INSERT INTO results VALUES (%d, %f)", result_list[i], result_distance_list[i]);
            ret = SPI_exec(query_buf.data, 0);
            resetStringInfo(&query_buf);
            if (ret != SPI_OK_INSERT)
            {
                elog(ERROR, "knn insert error, query: %s", query_buf.data);
            }
        }
    } else
    {
        //elog(INFO, "-------------------------------------------------------------------------");
        //elog(INFO, "candidate_list 满了");
        //elog(INFO, "-------------------------------------------------------------------------");
        // 通过 candidate_list, candidate_distance_list 构建结果
        for (int i = 0; i < num_of_candidates; i++)
        {
            resetStringInfo(&query_buf);
            appendStringInfo(&query_buf, "INSERT INTO results VALUES (%d, %f)", candidate_list[i], candidate_distance_list[i]);
            ret = SPI_exec(query_buf.data, 0);
            resetStringInfo(&query_buf);
            if (ret != SPI_OK_INSERT)
            {
                elog(ERROR, "knn insert error, query: %s", query_buf.data);
            }
        }
    }

    resetStringInfo(&query_buf);
    appendStringInfo(&query_buf, "INSERT INTO results (SELECT * FROM results ORDER BY distance ASC LIMIT %d)", top_k);
    ret = SPI_exec(query_buf.data, 0);
    resetStringInfo(&query_buf);
    if (ret != SPI_OK_INSERT)
    {
        elog(ERROR, "knn insert error, query: %s", query_buf.data);
    }

    pfree(frequency);
    pfree(result_list);
    pfree(result_distance_list);
    pfree(candidate_list);
    pfree(candidate_distance_list);
    pfree(query_object_hashes);

    free_matrix(hashfuncs, m);

    SPI_finish();
    PG_RETURN_INT32(top_k);
}

void 
read_hashfuncs(int m, int d, float *** hashfuncs)
{
    // ---------------------------------------
    //  util varaibles
    int ret;
    int proc;
	SPITupleTable *tuptable;
	HeapTuple	spi_tuple;
	TupleDesc	spi_tupdesc;
    bool isnull;
    ArrayType *val;
    float *tmp_hash_fun;
    int lenin;
    // ---------------------------------------

    StringInfoData query_buf;
    initStringInfo(&query_buf);

    for (int i = 1; i <= m; i++)
    {
    // 读出第 i 个 hash function
        appendStringInfo(&query_buf, "SELECT hash_func FROM hashfuncs WHERE id = %d", i);
        if ((ret = SPI_exec(query_buf.data, 0)) != SPI_OK_SELECT)
            elog(ERROR, "read_hashfuncs: SPI execution failed for query %s", query_buf.data);
        resetStringInfo(&query_buf);
        proc = SPI_processed;
        if (proc != 1)
            elog(ERROR, "read_hashfuncs: SPI execution 返回结果个数不为 1");
	    tuptable = SPI_tuptable;
        spi_tuple = tuptable->vals[0];
	    spi_tupdesc = tuptable->tupdesc;
    
        val = DatumGetArrayTypeP(SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull));

        lenin = ArrayGetNItems(ARR_NDIM(val), ARR_DIMS(val));
        // elog(INFO, "==== len in: %d ====", lenin);
        tmp_hash_fun = ( (float *) ARR_DATA_PTR(val) );
        for (int j = 0; j < d; j++)
        {   
            (*hashfuncs)[i-1][j] = tmp_hash_fun[j];
        }
    }
}


float 
update_radius(float old_radius, const float *query_object_hashes, int m_, float ratio_, float w_)
{
    char command[1024];
    int ret, proc;
    SPITupleTable *tuptable;
	HeapTuple	spi_tuple;
	TupleDesc	spi_tupdesc;
    bool isnull;
    // -------------------------------------------------------------------------
	//  find an array of projected distance which is closest to the query in
	//  each of <m> hash tables 
	// -------------------------------------------------------------------------
    
    //elog(INFO, "==================== old_radius: %f ========================", old_radius);
    float *list = palloc(sizeof(float) * m_);
	for (int i = 0; i < m_; i++)
    {
		sprintf(command, LONG_SQL_UPDATE_RADIUS, 
            i + 1, query_object_hashes[i] + (w_ * old_radius / 2), 
            i + 1, query_object_hashes[i] - (w_ * old_radius / 2), 
            query_object_hashes[i], query_object_hashes[i]);
        

        if ((ret = SPI_exec(command, 0)) != SPI_OK_SELECT)
            elog(ERROR, "updata_radius: SPI execution failed for query %s", command);
        proc = SPI_processed;
        if (proc != 1) 
            elog(ERROR, "updata_radius: no result from query: %s", command);
        // fix me 假如没有查到怎么办? 会出现这种情况吗?
        tuptable = SPI_tuptable;
        spi_tuple = tuptable->vals[0];
	    spi_tupdesc = tuptable->tupdesc;
        float di = (float) DatumGetFloat8(SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull));
        // 由于上面的 SQL 返回的是 pg 的 double 类型，需要强转为 float
        
        // #ifdef __DEBUG
        // if (old_radius == 0)
        // {
        //     elog(INFO, "// ------------------- 打印一个 di ------------------------------------------------------");
        //     elog(INFO, "d %d: %f", i, di);
        //     elog(INFO, "// ------------------- 打印生成这个 di 的sql ------------------------------------------------------");
        //     elog(INFO, "%s", command);
        // }
        // #endif
        
        list[i] = di;
	}


	qsort((void *) list, m_, sizeof(list[0]), compare_float_helper);

    // #ifdef __DEBUG
    // elog(INFO, "// ------------------- 开始 打印排序好的 di ------------------------------------------------------");
    // for (int i = 0; i < m_; i++)
    //     elog(INFO, "d %d: %f", i, list[i]);
    // elog(INFO, "// ------------------- 结束 打印排序好的 di ------------------------------------------------------");
    // #endif

    // -------------------------------------------------------------------------
	//  find the median distance and return the new radius
	// -------------------------------------------------------------------------
	// int num = (int) list.size();
	// if (num == 0) return ratio_ * old_radius;

    //elog(INFO, "// --------------------------------- m_: %d ----------------------------------------", m_);
	float dist = list[m_ / 2];
    //elog(INFO, "// --------------------------------- list[m_/2]: %f ----------------------------------------", dist);
	// if (num % 2 == 0) dist = (list[num / 2 - 1] + list[num / 2]) / 2.0f;
	// else dist = list[num / 2];
	
	// int kappa = (int) ceil(log(2.0f * dist / w_) / log(ratio_));
	// return pow(ratio_, kappa);
    pfree(list);
    int kappa = (int) ceil(log(2.0f * dist / w_) / log(ratio_));
    //elog(INFO, "// --------------------------------- kappa: %d ----------------------------------------", kappa);
    //elog(INFO, "// --------------------------------- ratio_: %f ----------------------------------------", ratio_);
    return pow(ratio_, kappa);
}

// ------------------------------------------------------
//  Calculate the radius for the first time
// ------------------------------------------------------
float 
init_radius(const float *query_object_hashes, int m_, float ratio_, float w_)
{
    return  update_radius(0, query_object_hashes,m_, ratio_, w_);
}


void
free_matrix(float ** matrix, int m)
{
    for (int i = 0; i < m; i++)
    {
        pfree(matrix[i]);
    }
    pfree(matrix);
}