#include "postgres.h"
#include "fmgr.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "def.h"
#include "random.h"
#include "aknnqalshzkf.h"
#include "util.h"

#include <stdio.h>
#include <stdlib.h>
#include <math.h>

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

const int   CANDIDATES     = 100;



// -----------------------------------------------------------------------------
//  function prototypes
// -----------------------------------------------------------------------------
PG_FUNCTION_INFO_V1(aknnqalsh_index);
PG_FUNCTION_INFO_V1(aknnqalsh_knn);
Datum aknnqalsh_index(PG_FUNCTION_ARGS);
Datum aknnqalsh_knn(PG_FUNCTION_ARGS);
void fill_data_struct_from_file(char *abs_path, struct Data *data_struct);
void fill_data_table(char *table_name, struct Data data);
void fill_param_table(char *dataset_name, int n, int d, float c, int m, int l);
void build_datahash_i_tables(struct Data data, struct Data hash_functions);

void insert_into_logs(int id, char * log);
void build_datarephash_i_tables(int m, int h);

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
    CREATE TABLE data (id int, coordinate real[]);\
    CREATE TABLE query (id int, coordinate real[]);\
    CREATE TABLE param (datasetname text, n int, d int, c real, m int, l int);\
    CREATE TABLE hashfuncs (id int, hash_func real[])";
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

	int w_ = w2;
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
    fill_param_table(dataset_name, n, d, c, m_, l_);
    
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
fill_param_table(char *dataset_name, int n, int d, float c, int m, int l)
{
    ereport(INFO,
					(errmsg("filling table: param")));
    char command_insert[1000];
    //SPI_connect();
    sprintf(command_insert, "INSERT INTO param VALUES ('%s', %d, %d, %f, %d, %d)", dataset_name, n, d, c, m, l);
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
    Datum		arrayDatum;
    ArrayType  *r;
    /* SPI (input tuple) support */
	SPITupleTable *tuptable;
	HeapTuple	spi_tuple;
	TupleDesc	spi_tupdesc;
    char *raw_array_string;
    bool isnull;
    ArrayType *val;
    float *din;
    int			lenin;

    // -------------------------------------------------------------------------
    //  init aknnqalsh_knn function arguments
    // -------------------------------------------------------------------------
    query_id = PG_GETARG_INT32(0);
    top_k = PG_GETARG_INT32(1);
    
    // -------------------------------------------------------------------------
    //  查询数据表query 获得查询对象的coordinate 保存在一个array中 也保存在一个string中 用于后续查询sql的拼接
    // -------------------------------------------------------------------------

    /* spi connect */
    if ((ret = SPI_connect()) < 0)
		elog(ERROR, "aknnqalsh_knn: SPI_connect returned %d", ret);

    /* init query buffer */
	initStringInfo(&query_buf);
    appendStringInfo(&query_buf, "SELECT coordinate FROM query WHERE id = %d", query_id);
    
    if ((ret = SPI_exec(query_buf.data, 0)) != SPI_OK_SELECT)
		elog(ERROR, "aknnqalsh: SPI execution failed for query %s",
			 query_buf.data);
    resetStringInfo(&query_buf);

    
    proc = SPI_processed;
	tuptable = SPI_tuptable;
    spi_tuple = tuptable->vals[0];
	spi_tupdesc = tuptable->tupdesc;
    
    //elog(INFO, "===== Before extract binary data =====");
    /* Extract the row data as C Strings */
    
	//raw_array_string = SPI_getvalue(spi_tuple, spi_tupdesc, 1);
    val = DatumGetArrayTypeP(SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull));

    lenin = ArrayGetNItems(ARR_NDIM(val), ARR_DIMS(val));
    // elog(INFO, "==== len in: %d ====", lenin);
    din = ( (float *) ARR_DATA_PTR(val) );

    // for (int i = 0; i < lenin; i++)
    //     elog(INFO, "%f,", din[i]);


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
    elog(INFO, "%s", datasetname);
    
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
    elog(INFO, "%d", n);

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
    elog(INFO, "%d", d);

    /* get c */
    appendStringInfo(&query_buf, "SELECT c FROM param");
	if ((ret = SPI_exec(query_buf.data, 0)) != SPI_OK_SELECT)
		elog(ERROR, "aknnqalsh_knn: SPI execution failed for query %s",
			 query_buf.data);
    resetStringInfo(&query_buf);
	proc = SPI_processed;
	tuptable = SPI_tuptable;
    spi_tuple = tuptable->vals[0];
	spi_tupdesc = tuptable->tupdesc;
    float c = DatumGetFloat4(SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull));
    elog(INFO, "%f", c);

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
    elog(INFO, "%d", m);

    /* get l */
    appendStringInfo(&query_buf, "SELECT l FROM param");
	if ((ret = SPI_exec(query_buf.data, 0)) != SPI_OK_SELECT)
		elog(ERROR, "aknnqalsh_knn: SPI execution failed for query %s",
			 query_buf.data);
    resetStringInfo(&query_buf);
	proc = SPI_processed;
	tuptable = SPI_tuptable;
    spi_tuple = tuptable->vals[0];
	spi_tupdesc = tuptable->tupdesc;
    int l = DatumGetInt32(SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull));
    elog(INFO, "%d", l);

    // -------------------------------------------------------------------------
    //  做range search， 找到 k 个最近邻
    // -------------------------------------------------------------------------

    // -------------------------------------------------------------------------
    //  将结果插入到query history表， 拼接返回值， 返回
    // -------------------------------------------------------------------------

    SPI_finish();
    PG_RETURN_INT32(top_k);
}

// uint64_t r_knn(				// r-k-NN search
// 	int   top_k,						// top-k value
// 	float R,							// limited search range
// 	const float *query,					// query object
// 	const int   *object_id, 			// object id mapping
// 	const char *data_folder,			// data folder
// 	struct MinK_List *list)					// k-NN results (return)
// {
//     // ---------------------------------------------------------------------
//     //  util variables
//     // ---------------------------------------------------------------------
    

//     int   *freq        = new int[n_pts_];
// 	bool  *checked     = new bool[n_pts_];
// 	bool  *bucket_flag = new bool[m_];
// 	bool  *range_flag  = new bool[m_];
// 	float *q_val       = new float[m_];
// 	float *data        = new float[dim_];
//     while (true) {
// 		// ---------------------------------------------------------------------
// 		//  step 1: initialize the stop condition for current round
// 		// ---------------------------------------------------------------------
// 		int num_bucket = 0;
// 		memset(bucket_flag, true, m_ * sizeof(bool));

// 		// ---------------------------------------------------------------------
// 		//  step 2: find frequent objects
// 		// ---------------------------------------------------------------------
// 		while (num_bucket < m_ && num_range < m_) {
// 			for (int i = 0; i < m_; ++i) {
// 				if (!bucket_flag[i]) continue;

// 				// -------------------------------------------------------------
// 				//  step 2.1: compute <ldist> and <rdist>
// 				// -------------------------------------------------------------
// 				Page *lptr = lptrs[i];
// 				Page *rptr = rptrs[i];

// 				float ldist = MAXREAL;
// 				float rdist = MAXREAL;
// 				if (lptr->size_ != -1) ldist = calc_dist(q_val[i], lptr);
// 				if (rptr->size_ != -1) rdist = calc_dist(q_val[i], rptr);

// 				// -------------------------------------------------------------
// 				//  step 2.2: determine the closer direction (left or right)
// 				//  and do collision counting to find frequent objects.
// 				//
// 				//  for the frequent object, we calc the L2 distance with
// 				//  query, and update the k-nn result.
// 				// -------------------------------------------------------------
// 				if (ldist < bucket && ldist < range && ldist <= rdist) {
// 					int count = lptr->size_;
// 					int end   = lptr->leaf_pos_;
// 					int start = end - count;

// 					for (int j = end; j > start; --j) {
// 						int id = lptr->leaf_node_->get_entry_id(j);
// 						if (++freq[id] > l_ && !checked[id]) {
// 							checked[id] = true;
// 							int oid = object_id[id];
// 							read_data_new_format(oid, dim_, B_, data_folder, data);

// 							float dist  = calc_lp_dist(dim_, p_, kdist, data, query);
// 							kdist = list->insert(dist, oid);
// 							if (++dist_io_ >= candidates) break;
// 						}
// 					}
// 					update_left_buffer(rptr, lptr);
// 				}
// 				else if (rdist < bucket && rdist < range && ldist > rdist) {
// 					int count = rptr->size_;
// 					int start = rptr->leaf_pos_;
// 					int end   = start + count;

// 					for (int j = start; j < end; ++j) {
// 						int id = rptr->leaf_node_->get_entry_id(j);
// 						if (++freq[id] > l_ && !checked[id]) {
// 							checked[id] = true;
// 							int oid = object_id[id];
// 							read_data_new_format(oid, dim_, B_, data_folder, data);

// 							float dist  = calc_lp_dist(dim_, p_, kdist, data, query);
// 							kdist = list->insert(dist, oid);
// 							if (++dist_io_ >= candidates) break;
// 						}
// 					}
// 					update_right_buffer(lptr, rptr);
// 				}
// 				else {
// 					bucket_flag[i] = false;
// 					num_bucket++;
// 					if (ldist >= range && rdist >= range && range_flag[i]) {
// 						range_flag[i] = false;
// 						num_range++;
// 					}
// 				}
// 				if (num_bucket >= m_ || num_range >= m_) break;
// 				if (dist_io_ >= candidates) break;
// 			}
// 			if (num_bucket >= m_ || num_range >= m_) break;
// 			if (dist_io_ >= candidates) break;
// 		}
// 		// ---------------------------------------------------------------------
// 		//  step 3: stop conditions 1 & 2
// 		// ---------------------------------------------------------------------
// 		if (dist_io_ >= candidates || num_range >= m_) break;

// 		// ---------------------------------------------------------------------
// 		//  step 4: auto-update <radius>
// 		// ---------------------------------------------------------------------
// 		radius = update_radius(radius, q_val, (const Page**) lptrs, 
// 			(const Page**) rptrs);
// 		bucket = radius * w_ / 2.0f;
// 	}
//     return 0;
// }