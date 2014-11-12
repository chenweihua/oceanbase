/* A Bison parser, made by GNU Bison 2.5.  */

/* Bison implementation for Yacc-like parsers in C
   
      Copyright (C) 1984, 1989-1990, 2000-2011 Free Software Foundation, Inc.
   
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.
   
   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output.  */
#define YYBISON 1

/* Bison version.  */
#define YYBISON_VERSION "2.5"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 1

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1

/* Using locations.  */
#define YYLSP_NEEDED 1



/* Copy the first part of user declarations.  */


#include <stdint.h>
#include "parse_node.h"
#include "parse_malloc.h"
#include "ob_non_reserved_keywords.h"
#include "common/ob_privilege_type.h"
#define YYDEBUG 1



/* Enabling traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 0
#endif

/* Enabling the token table.  */
#ifndef YYTOKEN_TABLE
# define YYTOKEN_TABLE 0
#endif


/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     NAME = 258,
     STRING = 259,
     INTNUM = 260,
     DATE_VALUE = 261,
     HINT_VALUE = 262,
     BOOL = 263,
     APPROXNUM = 264,
     NULLX = 265,
     UNKNOWN = 266,
     QUESTIONMARK = 267,
     SYSTEM_VARIABLE = 268,
     TEMP_VARIABLE = 269,
     EXCEPT = 270,
     UNION = 271,
     INTERSECT = 272,
     OR = 273,
     AND = 274,
     NOT = 275,
     COMP_NE = 276,
     COMP_GE = 277,
     COMP_GT = 278,
     COMP_EQ = 279,
     COMP_LT = 280,
     COMP_LE = 281,
     CNNOP = 282,
     LIKE = 283,
     BETWEEN = 284,
     IN = 285,
     IS = 286,
     MOD = 287,
     UMINUS = 288,
     ADD = 289,
     ANY = 290,
     ALL = 291,
     ALTER = 292,
     AS = 293,
     ASC = 294,
     BEGI = 295,
     BIGINT = 296,
     BINARY = 297,
     BOOLEAN = 298,
     BOTH = 299,
     BY = 300,
     CASCADE = 301,
     CASE = 302,
     CHARACTER = 303,
     CLUSTER = 304,
     COMMENT = 305,
     COMMIT = 306,
     CONSISTENT = 307,
     COLUMN = 308,
     COLUMNS = 309,
     CREATE = 310,
     CREATETIME = 311,
     CURRENT_USER = 312,
     CHANGE_OBI = 313,
     SWITCH_CLUSTER = 314,
     DATE = 315,
     DATETIME = 316,
     DEALLOCATE = 317,
     DECIMAL = 318,
     DEFAULT = 319,
     DELETE = 320,
     DESC = 321,
     DESCRIBE = 322,
     DISTINCT = 323,
     DOUBLE = 324,
     DROP = 325,
     DUAL = 326,
     ELSE = 327,
     END = 328,
     END_P = 329,
     ERROR = 330,
     EXECUTE = 331,
     EXISTS = 332,
     EXPLAIN = 333,
     FLOAT = 334,
     FOR = 335,
     FROM = 336,
     FULL = 337,
     FROZEN = 338,
     FORCE = 339,
     GLOBAL = 340,
     GLOBAL_ALIAS = 341,
     GRANT = 342,
     GROUP = 343,
     HAVING = 344,
     HINT_BEGIN = 345,
     HINT_END = 346,
     HOTSPOT = 347,
     IDENTIFIED = 348,
     IF = 349,
     INNER = 350,
     INTEGER = 351,
     INSERT = 352,
     INTO = 353,
     JOIN = 354,
     KEY = 355,
     LEADING = 356,
     LEFT = 357,
     LIMIT = 358,
     LOCAL = 359,
     LOCKED = 360,
     MEDIUMINT = 361,
     MEMORY = 362,
     MODIFYTIME = 363,
     MASTER = 364,
     NUMERIC = 365,
     OFFSET = 366,
     ON = 367,
     ORDER = 368,
     OPTION = 369,
     OUTER = 370,
     PARAMETERS = 371,
     PASSWORD = 372,
     PRECISION = 373,
     PREPARE = 374,
     PRIMARY = 375,
     READ_STATIC = 376,
     REAL = 377,
     RENAME = 378,
     REPLACE = 379,
     RESTRICT = 380,
     PRIVILEGES = 381,
     REVOKE = 382,
     RIGHT = 383,
     ROLLBACK = 384,
     KILL = 385,
     READ_CONSISTENCY = 386,
     SCHEMA = 387,
     SCOPE = 388,
     SELECT = 389,
     SESSION = 390,
     SESSION_ALIAS = 391,
     SET = 392,
     SHOW = 393,
     SMALLINT = 394,
     SNAPSHOT = 395,
     SPFILE = 396,
     START = 397,
     STATIC = 398,
     SYSTEM = 399,
     STRONG = 400,
     SET_MASTER_CLUSTER = 401,
     SET_SLAVE_CLUSTER = 402,
     SLAVE = 403,
     TABLE = 404,
     TABLES = 405,
     THEN = 406,
     TIME = 407,
     TIMESTAMP = 408,
     TINYINT = 409,
     TRAILING = 410,
     TRANSACTION = 411,
     TO = 412,
     UPDATE = 413,
     USER = 414,
     USING = 415,
     VALUES = 416,
     VARCHAR = 417,
     VARBINARY = 418,
     WHERE = 419,
     WHEN = 420,
     WITH = 421,
     WORK = 422,
     PROCESSLIST = 423,
     QUERY = 424,
     CONNECTION = 425,
     WEAK = 426,
     AUTO_INCREMENT = 427,
     CHUNKSERVER = 428,
     COMPRESS_METHOD = 429,
     CONSISTENT_MODE = 430,
     EXPIRE_INFO = 431,
     GRANTS = 432,
     JOIN_INFO = 433,
     MERGESERVER = 434,
     REPLICA_NUM = 435,
     ROOTSERVER = 436,
     ROW_COUNT = 437,
     SERVER = 438,
     SERVER_IP = 439,
     SERVER_PORT = 440,
     SERVER_TYPE = 441,
     STATUS = 442,
     TABLE_ID = 443,
     TABLET_BLOCK_SIZE = 444,
     TABLET_MAX_SIZE = 445,
     UNLOCKED = 446,
     UPDATESERVER = 447,
     USE_BLOOM_FILTER = 448,
     VARIABLES = 449,
     VERBOSE = 450,
     WARNINGS = 451
   };
#endif



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{


  struct _ParseNode *node;
  const struct _NonReservedKeyword *non_reserved_keyword;
  int    ival;



} YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
#endif

#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} YYLTYPE;
# define yyltype YYLTYPE /* obsolescent; will be withdrawn */
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


/* Copy the second part of user declarations.  */


#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>

#include "sql_parser.lex.h"

#define YYLEX_PARAM result->yyscan_info_

extern void yyerror(YYLTYPE* yylloc, ParseResult* p, char* s,...);

extern ParseNode* merge_tree(void *malloc_pool, ObItemType node_tag, ParseNode* source_tree);

extern ParseNode* new_terminal_node(void *malloc_pool, ObItemType type);

extern ParseNode* new_non_terminal_node(void *malloc_pool, ObItemType node_tag, int num, ...);

extern char* copy_expr_string(ParseResult* p, int expr_start, int expr_end);

#define ISSPACE(c) ((c) == ' ' || (c) == '\n' || (c) == '\r' || (c) == '\t' || (c) == '\f' || (c) == '\v')

#define malloc_terminal_node(node, malloc_pool, type) \
do \
{ \
  if ((node = new_terminal_node(malloc_pool, type)) == NULL) \
  { \
    yyerror(NULL, result, "No more space for malloc"); \
    YYABORT; \
  } \
} while(0)

#define malloc_non_terminal_node(node, malloc_pool, node_tag, ...) \
do \
{ \
  if ((node = new_non_terminal_node(malloc_pool, node_tag, ##__VA_ARGS__)) == NULL) \
  { \
    yyerror(NULL, result, "No more space for malloc"); \
    YYABORT; \
  } \
} while(0)

#define merge_nodes(node, malloc_pool, node_tag, source_tree) \
do \
{ \
  if (source_tree == NULL) \
  { \
    node = NULL; \
  } \
  else if ((node = merge_tree(malloc_pool, node_tag, source_tree)) == NULL) \
  { \
    yyerror(NULL, result, "No more space for merging nodes"); \
    YYABORT; \
  } \
} while (0)

#define dup_expr_string(str_ptr, result, expr_start, expr_end) \
  do \
  { \
    str_ptr = NULL; \
    int start = expr_start; \
    while (start <= expr_end && ISSPACE(result->input_sql_[start - 1])) \
      start++; \
    if (start >= expr_start \
      && (str_ptr = copy_expr_string(result, start, expr_end)) == NULL) \
    { \
      yyerror(NULL, result, "No more space for copying expression string"); \
      YYABORT; \
    } \
  } while (0)




#ifdef short
# undef short
#endif

#ifdef YYTYPE_UINT8
typedef YYTYPE_UINT8 yytype_uint8;
#else
typedef unsigned char yytype_uint8;
#endif

#ifdef YYTYPE_INT8
typedef YYTYPE_INT8 yytype_int8;
#elif (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
typedef signed char yytype_int8;
#else
typedef short int yytype_int8;
#endif

#ifdef YYTYPE_UINT16
typedef YYTYPE_UINT16 yytype_uint16;
#else
typedef unsigned short int yytype_uint16;
#endif

#ifdef YYTYPE_INT16
typedef YYTYPE_INT16 yytype_int16;
#else
typedef short int yytype_int16;
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif ! defined YYSIZE_T && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned int
# endif
#endif

#define YYSIZE_MAXIMUM ((YYSIZE_T) -1)

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(msgid) dgettext ("bison-runtime", msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(msgid) msgid
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(e) ((void) (e))
#else
# define YYUSE(e) /* empty */
#endif

/* Identity function, used to suppress warnings about constant conditions.  */
#ifndef lint
# define YYID(n) (n)
#else
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static int
YYID (int yyi)
#else
static int
YYID (yyi)
    int yyi;
#endif
{
  return yyi;
}
#endif

#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's `empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (YYID (0))
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
	     && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
	 || (defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL \
	     && defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yytype_int16 yyss_alloc;
  YYSTYPE yyvs_alloc;
  YYLTYPE yyls_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (yytype_int16) + sizeof (YYSTYPE) + sizeof (YYLTYPE)) \
      + 2 * YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)				\
    do									\
      {									\
	YYSIZE_T yynewbytes;						\
	YYCOPY (&yyptr->Stack_alloc, Stack, yysize);			\
	Stack = &yyptr->Stack_alloc;					\
	yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAXIMUM; \
	yyptr += yynewbytes / sizeof (*yyptr);				\
      }									\
    while (YYID (0))

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from FROM to TO.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(To, From, Count) \
      __builtin_memcpy (To, From, (Count) * sizeof (*(From)))
#  else
#   define YYCOPY(To, From, Count)		\
      do					\
	{					\
	  YYSIZE_T yyi;				\
	  for (yyi = 0; yyi < (Count); yyi++)	\
	    (To)[yyi] = (From)[yyi];		\
	}					\
      while (YYID (0))
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  161
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   2722

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  208
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  156
/* YYNRULES -- Number of rules.  */
#define YYNRULES  494
/* YYNRULES -- Number of states.  */
#define YYNSTATES  871

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   451

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,    36,     2,     2,
      40,    41,    34,    32,   207,    33,    42,    35,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   206,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,    38,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    37,    39,    43,
      44,    45,    46,    47,    48,    49,    50,    51,    52,    53,
      54,    55,    56,    57,    58,    59,    60,    61,    62,    63,
      64,    65,    66,    67,    68,    69,    70,    71,    72,    73,
      74,    75,    76,    77,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
      94,    95,    96,    97,    98,    99,   100,   101,   102,   103,
     104,   105,   106,   107,   108,   109,   110,   111,   112,   113,
     114,   115,   116,   117,   118,   119,   120,   121,   122,   123,
     124,   125,   126,   127,   128,   129,   130,   131,   132,   133,
     134,   135,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,   163,
     164,   165,   166,   167,   168,   169,   170,   171,   172,   173,
     174,   175,   176,   177,   178,   179,   180,   181,   182,   183,
     184,   185,   186,   187,   188,   189,   190,   191,   192,   193,
     194,   195,   196,   197,   198,   199,   200,   201,   202,   203,
     204,   205
};

#if YYDEBUG
/* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
   YYRHS.  */
static const yytype_uint16 yyprhs[] =
{
       0,     0,     3,     6,    10,    12,    14,    16,    18,    20,
      22,    24,    26,    28,    30,    32,    34,    36,    38,    40,
      42,    44,    46,    48,    50,    52,    54,    56,    58,    60,
      62,    63,    65,    69,    71,    75,    79,    81,    83,    85,
      87,    89,    91,    93,    95,    97,   101,   103,   105,   109,
     115,   117,   119,   121,   123,   126,   128,   131,   134,   138,
     142,   146,   150,   154,   158,   162,   164,   167,   170,   174,
     178,   182,   186,   190,   194,   198,   202,   206,   210,   214,
     218,   222,   226,   231,   235,   239,   242,   246,   251,   255,
     260,   264,   269,   275,   282,   286,   291,   295,   297,   301,
     307,   309,   310,   312,   315,   320,   323,   324,   329,   335,
     340,   347,   352,   356,   361,   363,   365,   367,   369,   371,
     373,   375,   381,   389,   391,   395,   399,   408,   412,   413,
     415,   419,   421,   427,   431,   433,   435,   437,   439,   441,
     444,   447,   449,   452,   454,   457,   460,   462,   465,   468,
     471,   474,   476,   478,   480,   483,   489,   493,   494,   498,
     499,   501,   502,   506,   507,   511,   512,   515,   516,   519,
     521,   524,   526,   529,   531,   535,   536,   540,   544,   548,
     552,   556,   560,   564,   568,   572,   576,   578,   579,   584,
     585,   588,   590,   594,   602,   607,   615,   616,   619,   621,
     623,   627,   628,   630,   634,   638,   644,   646,   650,   653,
     655,   659,   663,   665,   668,   672,   677,   683,   692,   694,
     696,   706,   711,   716,   721,   722,   725,   729,   734,   739,
     742,   745,   750,   751,   755,   757,   761,   762,   764,   767,
     769,   771,   776,   780,   783,   784,   786,   788,   790,   792,
     794,   796,   797,   799,   800,   803,   807,   812,   817,   822,
     826,   830,   834,   835,   839,   841,   842,   846,   848,   852,
     855,   856,   858,   860,   861,   864,   865,   867,   869,   871,
     874,   878,   880,   882,   886,   888,   892,   894,   898,   901,
     905,   908,   910,   916,   918,   922,   929,   935,   938,   941,
     944,   946,   948,   949,   953,   955,   957,   959,   961,   963,
     964,   968,   974,   980,   985,   990,   995,   998,  1003,  1007,
    1011,  1015,  1019,  1023,  1027,  1031,  1036,  1039,  1040,  1042,
    1045,  1050,  1052,  1054,  1055,  1056,  1059,  1062,  1063,  1065,
    1066,  1068,  1072,  1074,  1078,  1083,  1085,  1087,  1091,  1093,
    1097,  1103,  1110,  1113,  1114,  1118,  1122,  1124,  1128,  1133,
    1135,  1137,  1139,  1140,  1144,  1145,  1148,  1152,  1155,  1158,
    1163,  1164,  1166,  1167,  1169,  1171,  1178,  1180,  1184,  1187,
    1189,  1191,  1194,  1196,  1198,  1201,  1203,  1205,  1207,  1209,
    1211,  1212,  1214,  1216,  1222,  1225,  1226,  1231,  1233,  1235,
    1237,  1239,  1241,  1244,  1246,  1250,  1254,  1258,  1263,  1268,
    1274,  1280,  1284,  1286,  1288,  1290,  1294,  1297,  1298,  1300,
    1304,  1308,  1310,  1312,  1317,  1324,  1326,  1330,  1334,  1339,
    1344,  1350,  1352,  1353,  1355,  1357,  1358,  1362,  1366,  1370,
    1373,  1378,  1386,  1394,  1401,  1408,  1409,  1411,  1413,  1417,
    1427,  1430,  1431,  1435,  1439,  1443,  1444,  1446,  1448,  1450,
    1452,  1456,  1463,  1464,  1466,  1468,  1470,  1472,  1474,  1476,
    1478,  1480,  1482,  1484,  1486,  1488,  1490,  1492,  1494,  1496,
    1498,  1500,  1502,  1504,  1506,  1508,  1510,  1512,  1514,  1516,
    1518,  1520,  1522,  1524,  1526
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     209,     0,    -1,   210,    83,    -1,   210,   206,   211,    -1,
     211,    -1,   259,    -1,   252,    -1,   233,    -1,   230,    -1,
     229,    -1,   249,    -1,   293,    -1,   296,    -1,   332,    -1,
     335,    -1,   340,    -1,   345,    -1,   351,    -1,   343,    -1,
     303,    -1,   308,    -1,   310,    -1,   312,    -1,   315,    -1,
     325,    -1,   330,    -1,   319,    -1,   320,    -1,   321,    -1,
     322,    -1,    -1,   217,    -1,   212,   207,   217,    -1,   359,
      -1,   360,    42,   359,    -1,   360,    42,    34,    -1,     4,
      -1,     6,    -1,     5,    -1,     9,    -1,     8,    -1,    10,
      -1,    12,    -1,    14,    -1,    13,    -1,   145,    42,   359,
      -1,   213,    -1,   214,    -1,    40,   217,    41,    -1,    40,
     212,   207,   217,    41,    -1,   219,    -1,   224,    -1,   225,
      -1,   260,    -1,    86,   260,    -1,   215,    -1,    32,   216,
      -1,    33,   216,    -1,   216,    32,   216,    -1,   216,    33,
     216,    -1,   216,    34,   216,    -1,   216,    35,   216,    -1,
     216,    36,   216,    -1,   216,    38,   216,    -1,   216,    37,
     216,    -1,   215,    -1,    32,   217,    -1,    33,   217,    -1,
     217,    32,   217,    -1,   217,    33,   217,    -1,   217,    34,
     217,    -1,   217,    35,   217,    -1,   217,    36,   217,    -1,
     217,    38,   217,    -1,   217,    37,   217,    -1,   217,    26,
     217,    -1,   217,    25,   217,    -1,   217,    24,   217,    -1,
     217,    22,   217,    -1,   217,    23,   217,    -1,   217,    21,
     217,    -1,   217,    28,   217,    -1,   217,    20,    28,   217,
      -1,   217,    19,   217,    -1,   217,    18,   217,    -1,    20,
     217,    -1,   217,    31,    10,    -1,   217,    31,    20,    10,
      -1,   217,    31,     8,    -1,   217,    31,    20,     8,    -1,
     217,    31,    11,    -1,   217,    31,    20,    11,    -1,   217,
      29,   216,    19,   216,    -1,   217,    20,    29,   216,    19,
     216,    -1,   217,    30,   218,    -1,   217,    20,    30,   218,
      -1,   217,    27,   217,    -1,   260,    -1,    40,   212,    41,
      -1,    56,   220,   221,   223,    82,    -1,   217,    -1,    -1,
     222,    -1,   221,   222,    -1,   174,   217,   160,   217,    -1,
      81,   217,    -1,    -1,   361,    40,    34,    41,    -1,   361,
      40,   228,   217,    41,    -1,   361,    40,   212,    41,    -1,
     361,    40,   217,    47,   238,    41,    -1,   361,    40,   276,
      41,    -1,   361,    40,    41,    -1,   226,    40,   227,    41,
      -1,   191,    -1,   259,    -1,   252,    -1,   230,    -1,   229,
      -1,    45,    -1,    77,    -1,    74,    90,   289,   265,   253,
      -1,   167,   267,   289,   146,   231,   265,   253,    -1,   232,
      -1,   231,   207,   232,    -1,   359,    24,   217,    -1,    64,
     158,   234,   289,    40,   235,    41,   246,    -1,   103,    20,
      86,    -1,    -1,   236,    -1,   235,   207,   236,    -1,   237,
      -1,   129,   109,    40,   256,    41,    -1,   359,   238,   244,
      -1,   163,    -1,   148,    -1,   115,    -1,   105,    -1,    50,
      -1,    72,   239,    -1,   119,   239,    -1,    52,    -1,    88,
     240,    -1,   131,    -1,    78,   241,    -1,   162,   242,    -1,
      70,    -1,    57,   243,    -1,    51,   243,    -1,   171,   243,
      -1,   172,   243,    -1,    65,    -1,   117,    -1,    69,    -1,
     161,   242,    -1,    40,     5,   207,     5,    41,    -1,    40,
       5,    41,    -1,    -1,    40,     5,    41,    -1,    -1,   127,
      -1,    -1,    40,     5,    41,    -1,    -1,    40,     5,    41,
      -1,    -1,   244,   245,    -1,    -1,    20,    10,    -1,    10,
      -1,    73,   214,    -1,   181,    -1,   129,   109,    -1,   247,
      -1,   246,   207,   247,    -1,    -1,   187,   248,     4,    -1,
     185,   248,     4,    -1,   199,   248,     5,    -1,   198,   248,
       5,    -1,   197,   248,     5,    -1,   189,   248,     5,    -1,
     183,   248,     4,    -1,   202,   248,     8,    -1,   184,   248,
     152,    -1,    59,   248,     4,    -1,    24,    -1,    -1,    79,
     158,   250,   251,    -1,    -1,   103,    86,    -1,   288,    -1,
     251,   207,   288,    -1,   254,   107,   289,   255,   170,   257,
     253,    -1,   254,   107,   289,   259,    -1,   254,   107,   289,
      40,   256,    41,   259,    -1,    -1,   174,   217,    -1,   133,
      -1,   106,    -1,    40,   256,    41,    -1,    -1,   359,    -1,
     256,   207,   359,    -1,    40,   258,    41,    -1,   257,   207,
      40,   258,    41,    -1,   217,    -1,   258,   207,   217,    -1,
     261,   253,    -1,   260,    -1,    40,   261,    41,    -1,    40,
     260,    41,    -1,   262,    -1,   264,   275,    -1,   263,   279,
     275,    -1,   263,   278,   266,   275,    -1,   143,   267,   284,
     286,   274,    -1,   143,   267,   284,   286,    90,    80,   265,
     274,    -1,   264,    -1,   260,    -1,   143,   267,   284,   286,
      90,   287,   265,   277,   283,    -1,   263,    16,   284,   263,
      -1,   263,    17,   284,   263,    -1,   263,    15,   284,   263,
      -1,    -1,   173,   217,    -1,   173,     7,   217,    -1,   112,
     273,   120,   273,    -1,   120,   273,   112,   273,    -1,   112,
     273,    -1,   120,   273,    -1,   112,   273,   207,   273,    -1,
      -1,    99,   268,   100,    -1,   269,    -1,   268,   207,   269,
      -1,    -1,   270,    -1,   269,   270,    -1,   130,    -1,   101,
      -1,   140,    40,   272,    41,    -1,    40,   271,    41,    -1,
     271,   207,    -1,    -1,   180,    -1,   154,    -1,   152,    -1,
      92,    -1,     5,    -1,    12,    -1,    -1,   266,    -1,    -1,
      89,   167,    -1,   217,    90,   217,    -1,    53,   217,    90,
     217,    -1,   110,   217,    90,   217,    -1,   164,   217,    90,
     217,    -1,    53,    90,   217,    -1,   110,    90,   217,    -1,
     164,    90,   217,    -1,    -1,    97,    54,   212,    -1,   279,
      -1,    -1,   122,    54,   280,    -1,   281,    -1,   280,   207,
     281,    -1,   217,   282,    -1,    -1,    48,    -1,    75,    -1,
      -1,    98,   217,    -1,    -1,    45,    -1,    77,    -1,   217,
      -1,   217,   362,    -1,   217,    47,   362,    -1,    34,    -1,
     285,    -1,   286,   207,   285,    -1,   288,    -1,   287,   207,
     288,    -1,   289,    -1,   289,    47,   360,    -1,   289,   360,
      -1,   260,    47,   360,    -1,   260,   360,    -1,   290,    -1,
      40,   290,    41,    47,   360,    -1,   360,    -1,    40,   290,
      41,    -1,   288,   291,   108,   288,   121,   217,    -1,   288,
     108,   288,   121,   217,    -1,    91,   292,    -1,   111,   292,
      -1,   137,   292,    -1,   104,    -1,   124,    -1,    -1,    87,
     295,   294,    -1,   259,    -1,   229,    -1,   252,    -1,   230,
      -1,   204,    -1,    -1,   147,   159,   300,    -1,   147,    63,
      90,   289,   300,    -1,   147,    63,    30,   289,   300,    -1,
     147,   158,   196,   300,    -1,   147,   192,   196,   300,    -1,
     147,   299,   203,   300,    -1,   147,   141,    -1,   147,    64,
     158,   289,    -1,    76,   289,   301,    -1,    75,   289,   301,
      -1,   147,   205,   297,    -1,   147,   224,   205,    -1,   147,
     186,   298,    -1,   147,   125,   300,    -1,   147,   302,   177,
      -1,   112,     5,   207,     5,    -1,   112,     5,    -1,    -1,
     311,    -1,    89,    66,    -1,    89,    66,    40,    41,    -1,
      94,    -1,   144,    -1,    -1,    -1,    28,     4,    -1,   173,
     217,    -1,    -1,     4,    -1,    -1,    91,    -1,    64,   168,
     304,    -1,   305,    -1,   304,   207,   305,    -1,   306,   102,
      54,   307,    -1,     4,    -1,     4,    -1,    79,   168,   309,
      -1,   306,    -1,   309,   207,   306,    -1,   146,   126,   311,
      24,   307,    -1,    46,   168,   306,   102,    54,   307,    -1,
      89,   306,    -1,    -1,   132,   168,   314,    -1,   306,   166,
     306,    -1,   313,    -1,   314,   207,   313,    -1,    46,   168,
     306,   316,    -1,   114,    -1,   200,    -1,   176,    -1,    -1,
     175,    61,   149,    -1,    -1,    49,   317,    -1,   151,   165,
     318,    -1,    60,   317,    -1,   138,   317,    -1,   139,   323,
     324,     5,    -1,    -1,    94,    -1,    -1,   178,    -1,   179,
      -1,    96,   326,   121,   329,   166,   309,    -1,   327,    -1,
     326,   207,   327,    -1,    45,   328,    -1,    46,    -1,    64,
      -1,    64,   168,    -1,    74,    -1,    79,    -1,    96,   123,
      -1,   106,    -1,   167,    -1,   143,    -1,   133,    -1,   135,
      -1,    -1,    34,    -1,   360,    -1,   136,   326,   331,    90,
     309,    -1,   121,   329,    -1,    -1,   128,   333,    90,   334,
      -1,   362,    -1,   259,    -1,   252,    -1,   230,    -1,   229,
      -1,   146,   336,    -1,   337,    -1,   336,   207,   337,    -1,
      14,   338,   217,    -1,   359,   338,   217,    -1,    94,   359,
     338,   217,    -1,   144,   359,   338,   217,    -1,    95,    42,
     359,   338,   217,    -1,   145,    42,   359,   338,   217,    -1,
      13,   338,   217,    -1,   166,    -1,    24,    -1,    14,    -1,
      85,   333,   341,    -1,   169,   342,    -1,    -1,   339,    -1,
     342,   207,   339,    -1,   344,   128,   333,    -1,    71,    -1,
      79,    -1,    46,   158,   289,   346,    -1,    46,   158,   289,
     132,   166,   289,    -1,   347,    -1,   346,   207,   347,    -1,
      43,   348,   237,    -1,    79,   348,   359,   349,    -1,    46,
     348,   359,   350,    -1,   132,   348,   359,   166,   362,    -1,
      62,    -1,    -1,    55,    -1,   134,    -1,    -1,   146,    20,
      10,    -1,    79,    20,    10,    -1,   146,    73,   214,    -1,
      79,    73,    -1,    46,   153,   146,   353,    -1,    46,   153,
     352,    67,   118,    24,     4,    -1,    46,   153,   352,    68,
     118,    24,     4,    -1,    46,   153,   155,   118,    24,     4,
      -1,    46,   153,   156,   157,    24,     4,    -1,    -1,    93,
      -1,   354,    -1,   353,   207,   354,    -1,   359,    24,   214,
     355,   356,   195,    24,   357,   358,    -1,    59,     4,    -1,
      -1,   142,    24,   116,    -1,   142,    24,   150,    -1,   142,
      24,    53,    -1,    -1,   190,    -1,   201,    -1,   182,    -1,
     188,    -1,    58,    24,     5,    -1,   193,    24,     4,   194,
      24,     5,    -1,    -1,     3,    -1,   363,    -1,     3,    -1,
     363,    -1,     3,    -1,     3,    -1,   363,    -1,   181,    -1,
     182,    -1,   183,    -1,   184,    -1,   185,    -1,   186,    -1,
     187,    -1,   188,    -1,   189,    -1,   190,    -1,   191,    -1,
     192,    -1,   193,    -1,   194,    -1,   195,    -1,   196,    -1,
     198,    -1,   197,    -1,   199,    -1,   200,    -1,   201,    -1,
     202,    -1,   203,    -1,   204,    -1,   205,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   221,   221,   230,   237,   244,   245,   246,   247,   248,
     249,   250,   251,   252,   253,   254,   255,   256,   257,   258,
     259,   260,   261,   262,   263,   264,   265,   266,   267,   268,
     269,   279,   283,   290,   292,   298,   307,   308,   309,   310,
     311,   312,   313,   314,   315,   316,   320,   322,   324,   326,
     332,   340,   344,   348,   352,   360,   361,   365,   369,   370,
     371,   372,   373,   374,   375,   378,   379,   383,   387,   388,
     389,   390,   391,   392,   393,   394,   395,   396,   397,   398,
     399,   400,   401,   402,   406,   410,   414,   418,   422,   426,
     430,   434,   438,   442,   446,   450,   454,   461,   465,   470,
     478,   479,   483,   485,   490,   497,   498,   502,   516,   544,
     619,   635,   639,   661,   669,   676,   677,   678,   679,   683,
     687,   701,   715,   725,   729,   736,   750,   767,   770,   774,
     778,   785,   789,   798,   807,   809,   811,   813,   815,   817,
     826,   835,   837,   839,   841,   846,   853,   855,   862,   869,
     876,   883,   885,   887,   893,   905,   907,   910,   914,   915,
     919,   920,   924,   925,   929,   930,   934,   937,   941,   946,
     951,   953,   955,   960,   964,   969,   975,   980,   985,   990,
     995,  1000,  1005,  1010,  1015,  1021,  1029,  1030,  1041,  1051,
    1052,  1057,  1061,  1074,  1088,  1099,  1117,  1118,  1125,  1130,
    1138,  1143,  1147,  1148,  1155,  1159,  1165,  1166,  1180,  1190,
    1195,  1196,  1200,  1204,  1209,  1219,  1240,  1262,  1288,  1289,
    1293,  1319,  1341,  1363,  1389,  1390,  1394,  1401,  1409,  1417,
    1421,  1425,  1437,  1440,  1454,  1458,  1463,  1469,  1473,  1480,
    1484,  1488,  1493,  1500,  1505,  1511,  1515,  1519,  1523,  1529,
    1531,  1537,  1538,  1544,  1545,  1553,  1560,  1567,  1574,  1581,
    1592,  1603,  1618,  1619,  1626,  1627,  1631,  1638,  1640,  1645,
    1653,  1654,  1656,  1662,  1663,  1671,  1674,  1678,  1685,  1690,
    1698,  1706,  1716,  1720,  1727,  1729,  1734,  1738,  1742,  1746,
    1750,  1754,  1758,  1767,  1775,  1779,  1783,  1792,  1798,  1804,
    1810,  1817,  1818,  1828,  1836,  1837,  1838,  1839,  1843,  1844,
    1854,  1856,  1858,  1860,  1862,  1864,  1869,  1871,  1873,  1875,
    1877,  1881,  1894,  1898,  1902,  1910,  1919,  1929,  1933,  1935,
    1937,  1942,  1943,  1944,  1949,  1950,  1952,  1958,  1959,  1964,
    1965,  1974,  1980,  1984,  1990,  1996,  2002,  2014,  2020,  2024,
    2036,  2040,  2046,  2051,  2061,  2067,  2073,  2077,  2088,  2094,
    2099,  2112,  2117,  2121,  2126,  2130,  2136,  2147,  2159,  2171,
    2178,  2182,  2190,  2194,  2199,  2213,  2224,  2228,  2234,  2240,
    2245,  2250,  2255,  2260,  2265,  2270,  2275,  2280,  2285,  2292,
    2297,  2302,  2307,  2318,  2358,  2363,  2374,  2381,  2386,  2388,
    2390,  2392,  2403,  2411,  2415,  2422,  2428,  2435,  2442,  2449,
    2456,  2463,  2472,  2473,  2477,  2488,  2495,  2500,  2505,  2509,
    2522,  2530,  2532,  2543,  2549,  2560,  2564,  2571,  2576,  2582,
    2587,  2596,  2597,  2601,  2602,  2603,  2607,  2612,  2617,  2621,
    2635,  2641,  2648,  2655,  2662,  2672,  2675,  2683,  2687,  2694,
    2709,  2712,  2716,  2718,  2720,  2723,  2727,  2732,  2737,  2742,
    2750,  2754,  2759,  2770,  2772,  2789,  2791,  2808,  2812,  2814,
    2827,  2828,  2829,  2830,  2831,  2832,  2833,  2834,  2835,  2836,
    2837,  2838,  2839,  2840,  2841,  2842,  2843,  2844,  2845,  2846,
    2847,  2848,  2849,  2850,  2851
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || YYTOKEN_TABLE
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "NAME", "STRING", "INTNUM", "DATE_VALUE",
  "HINT_VALUE", "BOOL", "APPROXNUM", "NULLX", "UNKNOWN", "QUESTIONMARK",
  "SYSTEM_VARIABLE", "TEMP_VARIABLE", "EXCEPT", "UNION", "INTERSECT", "OR",
  "AND", "NOT", "COMP_NE", "COMP_GE", "COMP_GT", "COMP_EQ", "COMP_LT",
  "COMP_LE", "CNNOP", "LIKE", "BETWEEN", "IN", "IS", "'+'", "'-'", "'*'",
  "'/'", "'%'", "MOD", "'^'", "UMINUS", "'('", "')'", "'.'", "ADD", "ANY",
  "ALL", "ALTER", "AS", "ASC", "BEGI", "BIGINT", "BINARY", "BOOLEAN",
  "BOTH", "BY", "CASCADE", "CASE", "CHARACTER", "CLUSTER", "COMMENT",
  "COMMIT", "CONSISTENT", "COLUMN", "COLUMNS", "CREATE", "CREATETIME",
  "CURRENT_USER", "CHANGE_OBI", "SWITCH_CLUSTER", "DATE", "DATETIME",
  "DEALLOCATE", "DECIMAL", "DEFAULT", "DELETE", "DESC", "DESCRIBE",
  "DISTINCT", "DOUBLE", "DROP", "DUAL", "ELSE", "END", "END_P", "ERROR",
  "EXECUTE", "EXISTS", "EXPLAIN", "FLOAT", "FOR", "FROM", "FULL", "FROZEN",
  "FORCE", "GLOBAL", "GLOBAL_ALIAS", "GRANT", "GROUP", "HAVING",
  "HINT_BEGIN", "HINT_END", "HOTSPOT", "IDENTIFIED", "IF", "INNER",
  "INTEGER", "INSERT", "INTO", "JOIN", "KEY", "LEADING", "LEFT", "LIMIT",
  "LOCAL", "LOCKED", "MEDIUMINT", "MEMORY", "MODIFYTIME", "MASTER",
  "NUMERIC", "OFFSET", "ON", "ORDER", "OPTION", "OUTER", "PARAMETERS",
  "PASSWORD", "PRECISION", "PREPARE", "PRIMARY", "READ_STATIC", "REAL",
  "RENAME", "REPLACE", "RESTRICT", "PRIVILEGES", "REVOKE", "RIGHT",
  "ROLLBACK", "KILL", "READ_CONSISTENCY", "SCHEMA", "SCOPE", "SELECT",
  "SESSION", "SESSION_ALIAS", "SET", "SHOW", "SMALLINT", "SNAPSHOT",
  "SPFILE", "START", "STATIC", "SYSTEM", "STRONG", "SET_MASTER_CLUSTER",
  "SET_SLAVE_CLUSTER", "SLAVE", "TABLE", "TABLES", "THEN", "TIME",
  "TIMESTAMP", "TINYINT", "TRAILING", "TRANSACTION", "TO", "UPDATE",
  "USER", "USING", "VALUES", "VARCHAR", "VARBINARY", "WHERE", "WHEN",
  "WITH", "WORK", "PROCESSLIST", "QUERY", "CONNECTION", "WEAK",
  "AUTO_INCREMENT", "CHUNKSERVER", "COMPRESS_METHOD", "CONSISTENT_MODE",
  "EXPIRE_INFO", "GRANTS", "JOIN_INFO", "MERGESERVER", "REPLICA_NUM",
  "ROOTSERVER", "ROW_COUNT", "SERVER", "SERVER_IP", "SERVER_PORT",
  "SERVER_TYPE", "STATUS", "TABLE_ID", "TABLET_BLOCK_SIZE",
  "TABLET_MAX_SIZE", "UNLOCKED", "UPDATESERVER", "USE_BLOOM_FILTER",
  "VARIABLES", "VERBOSE", "WARNINGS", "';'", "','", "$accept", "sql_stmt",
  "stmt_list", "stmt", "expr_list", "column_ref", "expr_const",
  "simple_expr", "arith_expr", "expr", "in_expr", "case_expr", "case_arg",
  "when_clause_list", "when_clause", "case_default", "func_expr",
  "when_func", "when_func_name", "when_func_stmt", "distinct_or_all",
  "delete_stmt", "update_stmt", "update_asgn_list", "update_asgn_factor",
  "create_table_stmt", "opt_if_not_exists", "table_element_list",
  "table_element", "column_definition", "data_type", "opt_decimal",
  "opt_float", "opt_precision", "opt_time_precision", "opt_char_length",
  "opt_column_attribute_list", "column_attribute", "opt_table_option_list",
  "table_option", "opt_equal_mark", "drop_table_stmt", "opt_if_exists",
  "table_list", "insert_stmt", "opt_when", "replace_or_insert",
  "opt_insert_columns", "column_list", "insert_vals_list", "insert_vals",
  "select_stmt", "select_with_parens", "select_no_parens",
  "no_table_select", "select_clause", "simple_select", "opt_where",
  "select_limit", "opt_hint", "opt_hint_list", "hint_options",
  "hint_option", "opt_comma_list", "consistency_level", "limit_expr",
  "opt_select_limit", "opt_for_update", "parameterized_trim",
  "opt_groupby", "opt_order_by", "order_by", "sort_list", "sort_key",
  "opt_asc_desc", "opt_having", "opt_distinct", "projection",
  "select_expr_list", "from_list", "table_factor", "relation_factor",
  "joined_table", "join_type", "join_outer", "explain_stmt",
  "explainable_stmt", "opt_verbose", "show_stmt", "opt_limit",
  "opt_for_grant_user", "opt_scope", "opt_show_condition",
  "opt_like_condition", "opt_full", "create_user_stmt",
  "user_specification_list", "user_specification", "user", "password",
  "drop_user_stmt", "user_list", "set_password_stmt", "opt_for_user",
  "rename_user_stmt", "rename_info", "rename_list", "lock_user_stmt",
  "lock_spec", "opt_work", "opt_with_consistent_snapshot", "begin_stmt",
  "commit_stmt", "rollback_stmt", "kill_stmt", "opt_is_global", "opt_flag",
  "grant_stmt", "priv_type_list", "priv_type", "opt_privilege",
  "priv_level", "revoke_stmt", "opt_on_priv_level", "prepare_stmt",
  "stmt_name", "preparable_stmt", "variable_set_stmt", "var_and_val_list",
  "var_and_val", "to_or_eq", "argument", "execute_stmt", "opt_using_args",
  "argument_list", "deallocate_prepare_stmt", "deallocate_or_drop",
  "alter_table_stmt", "alter_column_actions", "alter_column_action",
  "opt_column", "opt_drop_behavior", "alter_column_behavior",
  "alter_system_stmt", "opt_force", "alter_system_actions",
  "alter_system_action", "opt_comment", "opt_config_scope", "server_type",
  "opt_cluster_or_address", "column_name", "relation_name",
  "function_name", "column_label", "unreserved_keyword", 0
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[YYLEX-NUM] -- Internal token number corresponding to
   token YYLEX-NUM.  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,    43,    45,    42,    47,    37,   287,    94,   288,
      40,    41,    46,   289,   290,   291,   292,   293,   294,   295,
     296,   297,   298,   299,   300,   301,   302,   303,   304,   305,
     306,   307,   308,   309,   310,   311,   312,   313,   314,   315,
     316,   317,   318,   319,   320,   321,   322,   323,   324,   325,
     326,   327,   328,   329,   330,   331,   332,   333,   334,   335,
     336,   337,   338,   339,   340,   341,   342,   343,   344,   345,
     346,   347,   348,   349,   350,   351,   352,   353,   354,   355,
     356,   357,   358,   359,   360,   361,   362,   363,   364,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,   421,   422,   423,   424,   425,
     426,   427,   428,   429,   430,   431,   432,   433,   434,   435,
     436,   437,   438,   439,   440,   441,   442,   443,   444,   445,
     446,   447,   448,   449,   450,   451,    59,    44
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   208,   209,   210,   210,   211,   211,   211,   211,   211,
     211,   211,   211,   211,   211,   211,   211,   211,   211,   211,
     211,   211,   211,   211,   211,   211,   211,   211,   211,   211,
     211,   212,   212,   213,   213,   213,   214,   214,   214,   214,
     214,   214,   214,   214,   214,   214,   215,   215,   215,   215,
     215,   215,   215,   215,   215,   216,   216,   216,   216,   216,
     216,   216,   216,   216,   216,   217,   217,   217,   217,   217,
     217,   217,   217,   217,   217,   217,   217,   217,   217,   217,
     217,   217,   217,   217,   217,   217,   217,   217,   217,   217,
     217,   217,   217,   217,   217,   217,   217,   218,   218,   219,
     220,   220,   221,   221,   222,   223,   223,   224,   224,   224,
     224,   224,   224,   225,   226,   227,   227,   227,   227,   228,
     228,   229,   230,   231,   231,   232,   233,   234,   234,   235,
     235,   236,   236,   237,   238,   238,   238,   238,   238,   238,
     238,   238,   238,   238,   238,   238,   238,   238,   238,   238,
     238,   238,   238,   238,   238,   239,   239,   239,   240,   240,
     241,   241,   242,   242,   243,   243,   244,   244,   245,   245,
     245,   245,   245,   246,   246,   246,   247,   247,   247,   247,
     247,   247,   247,   247,   247,   247,   248,   248,   249,   250,
     250,   251,   251,   252,   252,   252,   253,   253,   254,   254,
     255,   255,   256,   256,   257,   257,   258,   258,   259,   259,
     260,   260,   261,   261,   261,   261,   262,   262,   263,   263,
     264,   264,   264,   264,   265,   265,   265,   266,   266,   266,
     266,   266,   267,   267,   268,   268,   268,   269,   269,   270,
     270,   270,   270,   271,   271,   272,   272,   272,   272,   273,
     273,   274,   274,   275,   275,   276,   276,   276,   276,   276,
     276,   276,   277,   277,   278,   278,   279,   280,   280,   281,
     282,   282,   282,   283,   283,   284,   284,   284,   285,   285,
     285,   285,   286,   286,   287,   287,   288,   288,   288,   288,
     288,   288,   288,   289,   290,   290,   290,   291,   291,   291,
     291,   292,   292,   293,   294,   294,   294,   294,   295,   295,
     296,   296,   296,   296,   296,   296,   296,   296,   296,   296,
     296,   296,   296,   296,   296,   297,   297,   297,   298,   298,
     298,   299,   299,   299,   300,   300,   300,   301,   301,   302,
     302,   303,   304,   304,   305,   306,   307,   308,   309,   309,
     310,   310,   311,   311,   312,   313,   314,   314,   315,   316,
     316,   317,   317,   318,   318,   319,   319,   320,   321,   322,
     323,   323,   324,   324,   324,   325,   326,   326,   327,   327,
     327,   327,   327,   327,   327,   327,   327,   327,   327,   328,
     328,   329,   329,   330,   331,   331,   332,   333,   334,   334,
     334,   334,   335,   336,   336,   337,   337,   337,   337,   337,
     337,   337,   338,   338,   339,   340,   341,   341,   342,   342,
     343,   344,   344,   345,   345,   346,   346,   347,   347,   347,
     347,   348,   348,   349,   349,   349,   350,   350,   350,   350,
     351,   351,   351,   351,   351,   352,   352,   353,   353,   354,
     355,   355,   356,   356,   356,   356,   357,   357,   357,   357,
     358,   358,   358,   359,   359,   360,   360,   361,   362,   362,
     363,   363,   363,   363,   363,   363,   363,   363,   363,   363,
     363,   363,   363,   363,   363,   363,   363,   363,   363,   363,
     363,   363,   363,   363,   363
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     2,     3,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       0,     1,     3,     1,     3,     3,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     3,     1,     1,     3,     5,
       1,     1,     1,     1,     2,     1,     2,     2,     3,     3,
       3,     3,     3,     3,     3,     1,     2,     2,     3,     3,
       3,     3,     3,     3,     3,     3,     3,     3,     3,     3,
       3,     3,     4,     3,     3,     2,     3,     4,     3,     4,
       3,     4,     5,     6,     3,     4,     3,     1,     3,     5,
       1,     0,     1,     2,     4,     2,     0,     4,     5,     4,
       6,     4,     3,     4,     1,     1,     1,     1,     1,     1,
       1,     5,     7,     1,     3,     3,     8,     3,     0,     1,
       3,     1,     5,     3,     1,     1,     1,     1,     1,     2,
       2,     1,     2,     1,     2,     2,     1,     2,     2,     2,
       2,     1,     1,     1,     2,     5,     3,     0,     3,     0,
       1,     0,     3,     0,     3,     0,     2,     0,     2,     1,
       2,     1,     2,     1,     3,     0,     3,     3,     3,     3,
       3,     3,     3,     3,     3,     3,     1,     0,     4,     0,
       2,     1,     3,     7,     4,     7,     0,     2,     1,     1,
       3,     0,     1,     3,     3,     5,     1,     3,     2,     1,
       3,     3,     1,     2,     3,     4,     5,     8,     1,     1,
       9,     4,     4,     4,     0,     2,     3,     4,     4,     2,
       2,     4,     0,     3,     1,     3,     0,     1,     2,     1,
       1,     4,     3,     2,     0,     1,     1,     1,     1,     1,
       1,     0,     1,     0,     2,     3,     4,     4,     4,     3,
       3,     3,     0,     3,     1,     0,     3,     1,     3,     2,
       0,     1,     1,     0,     2,     0,     1,     1,     1,     2,
       3,     1,     1,     3,     1,     3,     1,     3,     2,     3,
       2,     1,     5,     1,     3,     6,     5,     2,     2,     2,
       1,     1,     0,     3,     1,     1,     1,     1,     1,     0,
       3,     5,     5,     4,     4,     4,     2,     4,     3,     3,
       3,     3,     3,     3,     3,     4,     2,     0,     1,     2,
       4,     1,     1,     0,     0,     2,     2,     0,     1,     0,
       1,     3,     1,     3,     4,     1,     1,     3,     1,     3,
       5,     6,     2,     0,     3,     3,     1,     3,     4,     1,
       1,     1,     0,     3,     0,     2,     3,     2,     2,     4,
       0,     1,     0,     1,     1,     6,     1,     3,     2,     1,
       1,     2,     1,     1,     2,     1,     1,     1,     1,     1,
       0,     1,     1,     5,     2,     0,     4,     1,     1,     1,
       1,     1,     2,     1,     3,     3,     3,     4,     4,     5,
       5,     3,     1,     1,     1,     3,     2,     0,     1,     3,
       3,     1,     1,     4,     6,     1,     3,     3,     4,     4,
       5,     1,     0,     1,     1,     0,     3,     3,     3,     2,
       4,     7,     7,     6,     6,     0,     1,     1,     3,     9,
       2,     0,     3,     3,     3,     0,     1,     1,     1,     1,
       3,     6,     0,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1
};

/* YYDEFACT[STATE-NAME] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
      30,     0,     0,   362,   362,     0,   421,     0,     0,     0,
     422,     0,   309,     0,   199,     0,     0,   198,     0,   362,
     370,   232,     0,   333,     0,   232,     0,     0,     4,     9,
       8,     7,    10,     6,     0,     5,   219,   196,   212,   265,
     218,    11,    12,    19,    20,    21,    22,    23,    26,    27,
      28,    29,    24,    25,    13,    14,    15,    18,     0,    16,
      17,   219,     0,   445,     0,     0,   361,   365,   367,   128,
       0,     0,   465,   470,   471,   472,   473,   474,   475,   476,
     477,   478,   479,   480,   481,   482,   483,   484,   485,   487,
     486,   488,   489,   490,   491,   492,   493,   494,   337,   293,
     466,   337,   189,     0,   468,   417,   397,   469,   308,     0,
     390,   379,   380,   382,   383,     0,   385,   388,   387,   386,
       0,   376,     0,     0,   395,   368,   371,   372,   236,   275,
     463,     0,     0,     0,     0,   353,     0,     0,   402,   403,
       0,   464,   467,     0,     0,   340,   331,   334,   316,   332,
       0,   334,   353,     0,   327,     0,     0,     0,     0,   364,
       0,     1,     2,    30,     0,     0,   208,   275,   275,   275,
       0,     0,   253,     0,   213,     0,   211,   210,   446,     0,
       0,     0,     0,     0,   345,     0,     0,     0,   341,   342,
       0,   224,   338,   319,   318,     0,     0,   348,   347,     0,
     415,   305,   307,   306,   304,   303,   389,   378,   381,   384,
       0,     0,     0,     0,   356,   354,     0,     0,   373,   374,
       0,   244,   240,   239,     0,     0,   234,   237,   276,   277,
       0,   413,   412,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   323,   334,
     310,     0,   322,   328,   334,     0,   320,   321,   334,   324,
       0,     0,   366,     0,     3,   201,   463,    36,    38,    37,
      40,    39,    41,    42,    44,    43,     0,     0,     0,     0,
     101,     0,     0,   480,    46,    47,    65,   197,    50,    51,
      52,     0,    53,    33,     0,   464,     0,     0,     0,     0,
       0,     0,   253,   214,   254,   420,   440,   447,     0,     0,
       0,     0,     0,   432,   432,   432,   432,   423,   425,     0,
     359,   360,   358,     0,     0,     0,     0,     0,   196,   190,
       0,   188,     0,   191,   286,   291,     0,   414,   418,   416,
     391,     0,   392,   377,   401,   400,   399,   398,   396,     0,
       0,   394,     0,   369,     0,     0,   233,     0,   238,   281,
     278,   282,   251,   411,   405,     0,     0,   352,     0,     0,
       0,   404,   406,   334,   334,   317,   335,   336,   313,   329,
     314,   326,   315,     0,   112,   119,     0,   120,     0,     0,
       0,    31,     0,     0,     0,     0,     0,     0,   194,    85,
      66,    67,     0,    31,    53,   100,     0,    54,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   232,   219,   223,   218,   221,   222,   270,   266,
     267,   249,   250,   229,   230,   215,     0,     0,     0,     0,
       0,     0,   431,     0,     0,     0,     0,     0,     0,     0,
     127,     0,   343,     0,     0,   225,   121,   219,     0,   291,
       0,     0,   290,   302,   300,     0,   302,   302,     0,     0,
     288,   349,     0,     0,   355,   357,   393,   242,   243,   248,
     247,   246,   245,     0,   235,     0,   279,     0,     0,   252,
     216,   407,     0,   346,   350,   408,     0,   312,   311,     0,
       0,   107,     0,     0,     0,     0,     0,     0,   109,     0,
       0,     0,     0,   111,   363,   224,   123,     0,     0,   202,
       0,     0,    48,     0,   106,   102,    45,    84,    83,     0,
       0,     0,    80,    78,    79,    77,    76,    75,    96,    81,
       0,     0,    55,     0,     0,    94,    97,    88,    86,    90,
       0,    68,    69,    70,    71,    72,    74,    73,     0,   118,
     117,   116,   115,    35,    34,   275,   271,   272,   269,     0,
       0,     0,     0,   448,   451,   443,   444,     0,     0,   427,
       0,     0,   435,   424,     0,   432,   426,   351,     0,     0,
     129,   131,   344,   226,   294,   192,   289,   301,   297,     0,
     298,   299,     0,   287,   419,   375,   241,   280,   224,   224,
     284,   283,   409,   410,   330,   325,   259,     0,   260,     0,
     261,     0,    32,   138,   165,   141,   165,   151,   153,   146,
     157,   161,   159,   137,   136,   152,   157,   143,   135,   163,
     163,   134,   165,   165,     0,   255,   108,     0,   196,     0,
     200,     0,     0,   196,    32,     0,     0,   103,     0,    82,
       0,    95,    56,    57,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    31,    89,    87,    91,   113,     0,   268,
     227,   231,   228,     0,   455,   441,   442,   167,     0,     0,
     429,   433,   434,   428,     0,     0,   175,     0,     0,     0,
       0,   251,     0,   262,   256,   257,   258,     0,   148,   147,
       0,   139,   160,   144,     0,   142,   140,     0,   154,   145,
     149,   150,   110,   124,   122,   125,   195,   203,   206,     0,
       0,   193,    49,     0,   105,    99,     0,    92,    58,    59,
      60,    61,    62,    64,    63,    98,     0,   450,     0,     0,
     133,     0,   439,     0,     0,   430,     0,   187,   187,   187,
     187,   187,   187,   187,   187,   187,   187,   126,   173,   130,
     292,   296,     0,   217,   285,     0,   273,     0,     0,     0,
       0,   204,     0,     0,   104,    93,     0,     0,     0,   169,
       0,     0,     0,   171,   166,   437,   436,   438,     0,   186,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   295,     0,     0,   220,   164,   156,     0,   158,   162,
     207,     0,   454,   452,   453,     0,   168,   170,   172,   132,
     185,   182,   184,   177,   176,   181,   180,   179,   178,   183,
     174,   263,   274,     0,   205,   458,   459,   456,   457,   462,
     155,     0,     0,   449,     0,     0,   460,     0,     0,     0,
     461
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    26,    27,    28,   390,   284,   285,   286,   553,   360,
     555,   288,   406,   534,   535,   668,   289,   290,   291,   568,
     392,    29,    30,   525,   526,    31,   187,   599,   600,   601,
     654,   721,   725,   723,   728,   718,   760,   804,   777,   778,
     810,    32,   196,   331,    33,   166,    34,   397,   528,   663,
     739,    35,   292,    37,    38,    39,    40,   328,   499,   129,
     225,   226,   227,   354,   493,   443,   500,   174,   393,   786,
     171,   172,   439,   440,   578,   824,   230,   361,   362,   619,
     620,   334,   335,   478,   608,    41,   205,   109,    42,   256,
     252,   156,   248,   193,   157,    43,   188,   189,   197,   504,
      44,   198,    45,   238,    46,   214,   215,    47,   322,    67,
     262,    48,    49,    50,    51,   127,   220,    52,   120,   121,
     207,   341,    53,   217,    54,   105,   348,    55,   138,   139,
     233,   338,    56,   200,   339,    57,    58,    59,   317,   318,
     457,   703,   700,    60,   182,   306,   307,   694,   759,   859,
     863,   293,   294,   158,   106,   295
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -547
static const yytype_int16 yypact[] =
{
     829,    19,   -64,   -93,   -93,    55,  -547,    18,  2091,  2091,
      75,  2123,  -112,   471,  -547,  2123,   -51,  -547,   471,   -93,
      25,    41,   481,   380,   -18,    41,   217,   -42,  -547,  -547,
    -547,  -547,  -547,  -547,   113,  -547,    -7,    48,  -547,    62,
      12,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,    96,  -547,
    -547,   185,   190,   253,  2091,   258,  -547,  -547,  -547,   133,
     258,  2091,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,   264,  -547,
    -547,   264,   170,   258,  -547,   107,  -547,  -547,  -547,   249,
     156,  -547,   125,  -547,  -547,   177,  -547,  -547,  -547,  -547,
     -59,  -547,   230,   258,   -22,  -547,  -547,   171,   179,    60,
    -547,    22,    22,  2173,   280,   236,  2173,   291,   131,  -547,
      22,  -547,  -547,    44,   195,  -547,  -547,    10,  -547,  -547,
     163,    10,   292,   192,   279,   188,   197,   229,   370,   237,
    2091,  -547,  -547,   829,  2091,  1684,  -547,    60,    60,    60,
     360,   146,   152,   248,  -547,  2123,  -547,  -547,  -547,  2173,
     299,   262,   318,   231,  -547,   -54,   406,  2091,   221,  -547,
     327,   265,  -547,  -547,  -547,   353,   580,  -547,   239,   431,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    1264,   471,   249,   286,  -547,   255,  1264,   363,  -547,  -547,
     458,  -547,  -547,  -547,   426,   -16,   179,  -547,  -547,  -547,
    1119,  -547,  -547,  1684,  1684,    22,  2173,   258,   444,    22,
    2173,  1748,  1684,  2091,  2091,  2091,   472,  1684,  -547,    10,
    -547,    67,  -547,  -547,    10,   474,  -547,  -547,    10,  -547,
     854,   419,  -547,   337,  -547,    27,    30,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  1684,  1684,  1684,  1152,
    1684,   445,   446,   453,  -547,  -547,  -547,  2665,  -547,  -547,
    -547,   462,  -547,  -547,   456,   464,    51,    51,    51,  1684,
      91,    91,   415,  -547,  -547,  -547,   315,  -547,   499,   501,
     502,   418,   434,   475,   475,   475,    -1,   333,  -547,   495,
    -547,  -547,  -547,   467,   518,   258,   506,  1355,    48,  -547,
    1030,   355,  1920,   276,  1970,  -547,   258,  -547,  -547,   356,
    -547,   402,  -547,  -547,  -547,  -547,  -547,  -547,  -547,   258,
     258,  -547,   258,  -547,   -14,   225,  -547,   179,  -547,  -547,
    1058,  -547,   -39,  2665,  2665,  1684,    22,  -547,   561,  1684,
      22,  -547,  2665,    10,    10,  -547,  -547,  2665,  -547,   530,
    -547,   364,  -547,   532,  -547,  -547,  1388,  -547,  1476,  1596,
     -10,  2484,  1684,   533,   429,  2173,  1811,   409,  -547,  2684,
    -547,  -547,   373,  2593,   254,  2665,   407,  -547,  2173,  1684,
    1684,   394,  1684,  1684,  1684,  1684,  1684,  1684,  1684,  1684,
    1717,   544,   387,  1684,  1684,  1684,  1684,  1684,  1684,  1684,
     249,  2021,    41,  -547,   586,  -547,   586,  -547,  2535,   398,
    -547,  -547,  -547,   -68,   497,  -547,  2173,   147,   604,   606,
     587,   589,  -547,  2173,  2173,  2173,  2091,  2173,   232,   561,
    -547,  2053,  -547,   561,  1684,  2665,  -547,  1849,   276,   583,
     580,  2091,  -547,   498,  -547,   580,   498,   498,   542,  2091,
    -547,  -547,   431,   258,  -547,  -547,   239,  -547,  -547,  -547,
    -547,  -547,  -547,   611,   179,  2123,  -547,  1888,  1119,  -547,
    -547,  2665,  1684,  -547,  -547,  2665,  1684,  -547,  -547,   612,
     649,  -547,  1684,  2109,  1684,  2312,  1684,  2514,  -547,  1684,
    2329,  1684,  2617,  -547,  -547,  -110,  -547,   632,    -5,  -547,
     617,  1684,  -547,  1684,   -24,  -547,  -547,  2565,  2684,  1684,
    1717,   544,   559,   559,   559,   559,   559,   559,   599,   610,
    1717,  1717,  -547,   399,  1152,  -547,  -547,  -547,  -547,  -547,
     326,   422,   422,   620,   620,   620,   620,  -547,   618,  -547,
    -547,  -547,  -547,  -547,  -547,    60,  -547,  -547,  -547,  1684,
      91,    91,    91,  -547,   602,  -547,  -547,   684,   685,  -547,
    2329,   -15,    61,  -547,   524,   475,  -547,  -547,   584,    -2,
    -547,  -547,  -547,  2665,   645,   276,  -547,  -547,  -547,     7,
    -547,  -547,   580,  -547,  -547,   239,  -547,  -547,   265,  -107,
     276,  -547,  2665,  2665,  -547,  -547,  2665,  1684,  2665,  1684,
    2665,  1684,  2665,  -547,   654,  -547,   654,  -547,  -547,  -547,
     655,   569,   657,  -547,  -547,  -547,   655,  -547,  -547,   659,
     659,  -547,   654,   654,   663,  2665,  -547,  2173,    48,  1684,
      19,  2173,  1684,  -105,  2641,  1250,  1684,  -547,   623,   610,
     477,  -547,  -547,  -547,  1717,  1717,  1717,  1717,  1717,  1717,
    1717,  1717,     1,  2665,  -547,  -547,  -547,  -547,  1119,  -547,
    -547,  -547,  -547,   696,   567,  -547,  -547,  -547,    65,    68,
    -547,  -547,  -547,  -547,  2123,   673,   173,  2053,  2091,  1684,
     257,   146,   580,   619,  2665,  2665,  2665,   710,  -547,  -547,
     712,  -547,  -547,  -547,   713,  -547,  -547,   715,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  2665,  -547,  -547,  2665,     2,
     681,  -547,  -547,  1684,  2665,  -547,  1717,   496,   581,   581,
     686,   686,   686,   686,  -547,  -547,   -35,  -547,   698,   531,
      20,   717,  -547,   718,   147,  -547,  2173,   701,   701,   701,
     701,   701,   701,   701,   701,   701,   701,   522,  -547,  -547,
    -547,  2665,  1684,  -547,   276,   676,   635,   702,     4,   703,
     711,  -547,  1684,  1684,  2665,   496,   580,    90,   730,  -547,
     732,   147,   646,  -547,  -547,  -547,  -547,  -547,     8,  -547,
     752,   753,   607,   754,   783,   784,   786,   787,   788,   780,
     173,  2665,  1684,  1684,  -547,  -547,  -547,   789,  -547,  -547,
    2665,     9,  -547,  -547,  -547,   139,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,   588,  2665,   765,  -547,  -547,  -547,  -547,  -547,    -4,
    -547,   785,   790,  -547,   803,   806,  -547,   621,   792,   808,
    -547
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -547,  -547,  -547,   648,  -271,  -547,  -432,  -373,  -428,   322,
     277,  -547,  -547,  -547,   283,  -547,   796,  -547,  -547,  -547,
    -547,   -91,   -88,  -547,   164,  -547,  -547,  -547,   115,   367,
     235,   180,  -547,  -547,   181,  -546,  -547,  -547,  -547,     3,
      29,  -547,  -547,  -547,   -86,  -311,  -547,  -547,    63,  -547,
      34,   -99,     0,     5,  -547,   222,   246,  -390,   661,   -21,
    -547,   473,  -200,  -547,  -547,  -266,   122,  -128,  -547,  -547,
    -547,  -547,  -547,   256,  -547,  -547,  -155,   339,   151,  -547,
    -185,    16,   510,  -547,   -73,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -129,   741,  -547,  -547,  -547,   519,   -37,  -349,
    -547,  -296,  -547,   693,  -547,   500,  -547,  -547,  -547,   238,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,   828,   636,
    -547,   633,  -547,  -547,  -547,    17,  -547,  -547,  -547,   613,
    -103,   366,  -547,  -547,  -547,  -547,  -547,  -547,  -547,   393,
     233,  -547,  -547,  -547,  -547,  -547,   410,  -547,  -547,  -547,
    -547,    46,    11,  -547,  -353,    -6
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -468
static const yytype_int16 yytable[] =
{
      36,    61,   100,   100,   160,   107,    62,   496,   402,   107,
     204,   333,   296,   297,   298,   584,   141,   466,   201,    99,
      99,   202,   250,   203,    98,   101,   358,   487,   185,   234,
     799,   518,   122,   190,  -209,   444,   660,   242,   246,   706,
     800,   162,   755,   791,   303,   826,   231,   552,   319,   839,
     854,   497,   580,  -253,   861,   796,   486,   666,   100,     1,
     320,   452,   210,   327,   698,   100,   327,   396,   140,   165,
    -467,   184,  -465,   300,   243,    99,  -209,   167,   168,   169,
     183,   301,    99,    66,   356,   761,   213,   191,   763,    63,
     719,     1,   108,   801,    64,  -253,   441,   657,   473,   216,
     712,   173,   740,   442,    65,   228,   730,   731,    71,    36,
     597,   474,   670,   347,   602,   475,   701,   123,   476,   126,
     378,   344,   672,   673,   345,   380,   346,   141,   709,   382,
     141,   699,   365,   379,   244,   658,   369,   229,   762,   581,
     128,   764,   617,   832,   477,   468,   321,   159,   211,   802,
     533,   267,   268,   269,   100,   270,   271,   272,   100,   273,
     274,   275,    21,    36,   163,   456,   398,   552,   498,   107,
      21,    99,   498,   141,   445,    99,   263,   552,   552,   235,
     265,   100,   239,   247,   170,   211,  -253,   615,   232,   862,
     100,   357,   305,   488,   432,   702,   332,   519,    99,  -209,
     367,   803,   661,   324,   100,   707,   833,    99,   519,   792,
     100,   827,    36,    69,   367,   661,   792,   161,  -253,   221,
     164,   342,   165,    70,   175,   308,   176,   342,   711,   713,
     141,   177,   767,   102,   141,   141,   186,   100,   100,   100,
     834,   173,    68,   103,   507,   508,   747,   748,   749,   750,
     751,   752,   753,   754,    99,    99,    99,   125,   300,   373,
     374,   375,   184,   502,  -264,    36,   301,   506,   192,  -219,
    -219,  -219,  -264,   195,   313,   313,   199,   314,   314,   404,
     222,   407,   366,   682,    62,   605,   370,   140,   190,     1,
     609,   206,   282,   208,   358,   176,   433,   433,   433,   481,
     209,   552,   552,   552,   552,   552,   552,   552,   552,   223,
     315,   315,   484,   213,   690,   691,   692,   489,   795,   224,
     212,   855,   236,     7,   100,   237,   100,   856,   100,   857,
     467,   572,   807,   240,   684,    62,   685,   686,   241,   569,
     858,    99,   570,   472,   571,   480,   178,   734,   473,   218,
     219,   765,   741,   245,   107,    14,   768,   769,   770,   249,
     771,   474,   772,   316,   595,   475,  -219,   473,   476,   837,
     773,   774,   775,   552,  -219,   776,  -219,   490,   782,   491,
     474,   251,    17,   142,   475,   311,   312,   476,   254,   141,
     141,   255,    21,   257,   477,   557,    61,   558,   559,   179,
     258,    62,   141,   610,   611,   492,   259,   560,   180,   181,
     260,   575,   261,   477,   299,   304,    25,   309,   674,   310,
     688,   556,   539,   540,   541,   141,   323,   710,   325,   326,
      36,   675,   676,   677,   678,   679,   680,   681,   327,   329,
     141,   527,   529,   143,   144,   337,   336,   141,   141,   141,
     100,   141,   349,   352,   536,   141,   425,   426,   427,   428,
     429,   100,   350,   353,   100,   100,   355,    99,   368,   100,
     332,   145,   593,   100,   146,   332,   376,   574,   472,   381,
     394,    99,   606,   395,   130,     1,    99,   287,   408,   107,
     613,   100,   308,  -114,   131,   132,   746,   332,   431,   590,
     591,   592,   430,   594,   173,   147,  -466,   590,    99,   675,
     676,   677,   678,   679,   680,   681,   110,   111,   434,   436,
     437,   148,   446,   447,   149,   448,   449,   784,   675,   676,
     677,   678,   679,   680,   681,   112,   450,   452,   150,   151,
     458,   556,   435,   435,   435,   113,   453,   454,   455,   459,
     114,   851,   451,   460,   404,   363,   364,  -339,   461,    62,
     463,   736,   470,   482,   372,   503,   152,   115,   483,   377,
     509,   510,   153,   511,   523,   133,   134,   116,   524,   530,
     531,   533,   391,    72,   554,   154,   418,   419,   420,   421,
     422,   423,   424,   425,   426,   427,   428,   429,   399,   400,
     401,   403,   405,   169,   117,   579,   100,   135,   585,   582,
     586,   587,   332,   588,   118,   677,   678,   679,   680,   681,
     330,   438,   607,    99,   604,   136,   137,   419,   420,   421,
     422,   423,   424,   425,   426,   427,   428,   429,   119,   420,
     421,   422,   423,   424,   425,   426,   427,   428,   429,   465,
     612,   141,   616,   624,   625,   141,   659,   662,   429,   687,
      36,   693,    73,    74,    75,    76,    77,    78,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,    92,    93,    94,    95,    96,    97,   501,   695,   696,
     704,   505,   708,   705,   717,   720,   722,   724,   107,   727,
     757,   141,   100,   527,   732,   745,   100,   737,   513,   758,
     515,   517,   332,   766,   522,   787,   785,   788,   789,   780,
     790,   793,   797,    99,   681,   809,   798,   805,   806,   820,
     822,   537,   538,   823,   542,   543,   544,   545,   546,   547,
     548,   549,   836,   825,   828,   561,   562,   563,   564,   565,
     566,   567,   829,   590,   835,   838,   840,   841,   843,   842,
     141,    73,    74,    75,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
      92,    93,    94,    95,    96,    97,   603,   844,   849,   845,
     100,   846,   847,   848,   853,   519,   332,   811,   812,   813,
     814,   815,   816,   817,   818,   819,   860,    99,   866,   864,
     867,   264,   529,   870,   865,   868,   869,   667,   671,   155,
     589,   733,   779,   850,   622,   697,   726,   831,   623,   808,
     494,   729,   302,   783,   626,   689,   628,   621,   630,   756,
     469,   632,   194,   655,   462,   253,   124,   343,   614,   351,
     485,   596,     0,   664,   371,   665,   583,   266,   267,   268,
     269,   669,   270,   271,   272,     0,   273,   274,   275,     1,
       0,     0,     0,     0,   276,     2,   683,     0,     3,     0,
       0,     0,     0,     0,     0,     0,   277,   278,   383,     4,
       0,     0,     0,     5,   279,   384,     0,     0,     0,   385,
       6,   438,     0,     7,     8,     9,     0,   386,    10,     0,
     280,     0,     0,     0,    11,     0,    12,     0,     0,     0,
       0,     0,     0,     0,     0,    13,     0,     0,     0,     0,
       0,   387,     0,     0,     0,    14,     0,     0,     0,     0,
     281,     0,     0,     0,     0,     0,     0,     0,     0,   714,
       0,   715,     0,   716,     0,     0,     0,    15,     0,     0,
       0,    16,    17,     0,   388,    18,     0,    19,    20,     0,
       0,     0,    21,     0,     0,    22,    23,     0,     0,     0,
      24,   735,     0,     0,   738,     0,     0,     0,   744,     0,
       0,     0,     0,     0,     0,     0,    25,     0,     0,   282,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   389,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   781,     0,    72,     0,    73,    74,    75,    76,    77,
      78,    79,    80,    81,    82,   283,    84,    85,    86,    87,
      88,    89,    90,    91,    92,    93,    94,    95,    96,    97,
       0,   104,     0,     0,     0,   794,     0,     0,     0,     0,
     330,     0,     0,     0,     0,     0,   409,   410,   411,   412,
     413,   414,   415,   416,   417,   418,   419,   420,   421,   422,
     423,   424,   425,   426,   427,   428,   429,     0,     0,     0,
       0,     0,     0,     0,   821,   495,     0,     0,     0,     0,
       0,     0,     0,     0,   830,   738,     0,     0,     0,     0,
       0,     0,   266,   267,   268,   269,     0,   270,   271,   272,
       0,   273,   274,   275,     0,     0,     0,     0,     0,   276,
       0,     0,     0,     0,   683,   852,     0,     0,     0,     0,
       0,   277,   278,   359,     0,   266,   267,   268,   269,   279,
     270,   271,   272,     0,   273,   274,   275,     0,     0,     0,
       0,     0,   276,    21,     0,   280,     0,     0,     0,     0,
       0,     0,     0,     0,   277,   278,     0,     0,     0,     0,
       0,     0,   279,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   281,     0,     0,   280,     0,
       0,    73,    74,    75,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
      92,    93,    94,    95,    96,    97,     0,     0,   281,    73,
      74,    75,    76,    77,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
      94,    95,    96,    97,   282,     0,     0,    72,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
     421,   422,   423,   424,   425,   426,   427,   428,   429,     0,
       0,     0,     0,     0,     0,    21,     0,   282,   340,     0,
      73,    74,    75,    76,    77,    78,    79,    80,    81,    82,
     283,    84,    85,    86,    87,    88,    89,    90,    91,    92,
      93,    94,    95,    96,    97,     0,     0,     0,     0,     0,
       0,     0,     0,    73,    74,    75,    76,    77,    78,    79,
      80,    81,    82,   283,    84,    85,    86,    87,    88,    89,
      90,    91,    92,    93,    94,    95,    96,    97,   266,   267,
     268,   269,   464,   270,   271,   272,     0,   273,   274,   275,
       0,     0,     0,     0,     0,   276,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   277,   278,     0,
       0,   266,   267,   268,   269,   279,   270,   271,   272,     0,
     273,   274,   275,     0,     0,     0,     0,     0,   276,     0,
     743,   280,     0,     0,     0,     0,     0,     0,     0,     0,
     277,   278,     0,     0,     0,     0,     0,     0,   279,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   281,     0,     0,   280,    73,    74,    75,    76,    77,
      78,    79,    80,    81,    82,    83,    84,    85,    86,    87,
      88,    89,    90,    91,    92,    93,    94,    95,    96,    97,
       0,     0,     0,     0,   281,     0,     0,     0,   512,   266,
     267,   268,   269,     0,   270,   271,   272,     0,   273,   274,
     275,     0,     0,     0,     0,     0,   276,     0,     0,     0,
     282,     0,     0,     0,     0,     0,     0,     0,   277,   278,
       0,     0,     0,     0,     0,     0,   279,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   280,   282,     0,     0,    73,    74,    75,    76,
      77,    78,    79,    80,    81,    82,   283,    84,    85,    86,
      87,    88,    89,    90,    91,    92,    93,    94,    95,    96,
      97,     0,   281,     0,     0,     0,   514,     0,     0,    73,
      74,    75,    76,    77,    78,    79,    80,    81,    82,   283,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
      94,    95,    96,    97,     0,     0,     0,     0,     0,   266,
     267,   268,   269,     0,   270,   271,   272,     0,   273,   274,
     275,     0,     0,     0,     0,     0,   276,     0,     0,     0,
       0,   282,     0,     0,     0,     0,     0,     0,   277,   278,
       0,     0,     0,     0,     0,     0,   279,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   280,     0,     0,     0,     0,    73,    74,    75,
      76,    77,    78,    79,    80,    81,    82,   283,    84,    85,
      86,    87,    88,    89,    90,    91,    92,    93,    94,    95,
      96,    97,   281,     0,     0,     0,   516,   266,   267,   268,
     269,     0,   270,   271,   272,     0,   273,   274,   275,     0,
       0,     0,     0,     0,   276,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   277,   278,     0,     0,
     266,   267,   268,   269,   279,   270,   271,   272,     0,   273,
     274,   275,     0,     0,     0,     0,     0,     0,     0,     0,
     280,   282,     0,     0,     0,     0,     0,     0,     0,   550,
     551,   130,     0,     0,     0,     0,     0,   279,     0,     0,
       0,   131,   132,     0,     0,     0,     0,     0,     0,     0,
     281,     0,     0,   280,     0,     0,     0,    73,    74,    75,
      76,    77,    78,    79,    80,    81,    82,   283,    84,    85,
      86,    87,    88,    89,    90,    91,    92,    93,    94,    95,
      96,    97,     0,   281,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   130,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   282,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   133,   134,     0,     0,     0,     0,     0,     0,
       0,     1,    72,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   282,     0,     0,    73,    74,    75,    76,    77,
      78,    79,    80,    81,    82,   283,    84,    85,    86,    87,
      88,    89,    90,    91,    92,    93,    94,    95,    96,    97,
     176,    72,   136,   137,     0,     0,   471,     0,    73,    74,
      75,    76,    77,    78,    79,    80,    81,    82,   283,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,    96,    97,    72,     0,     0,     0,     0,   330,    73,
      74,    75,    76,    77,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
      94,    95,    96,    97,    21,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   471,   618,     0,
       0,     0,     0,    72,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    73,    74,    75,    76,    77,    78,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,    92,    93,    94,    95,    96,    97,   479,     0,     0,
       0,     0,     0,     0,   130,     0,     0,     0,     0,     0,
      73,    74,    75,    76,    77,    78,    79,    80,    81,    82,
      83,    84,    85,    86,    87,    88,    89,    90,    91,    92,
      93,    94,    95,    96,    97,   573,   130,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    73,
      74,    75,    76,    77,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
      94,    95,    96,    97,    72,     0,     0,     0,     0,     0,
       0,    73,    74,    75,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
      92,    93,    94,    95,    96,    97,   104,   409,   410,   411,
     412,   413,   414,   415,   416,   417,   418,   419,   420,   421,
     422,   423,   424,   425,   426,   427,   428,   429,     0,     0,
       0,    73,    74,    75,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
      92,    93,    94,    95,    96,    97,   130,     0,     0,     0,
       0,     0,   598,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   627,
       0,     0,    73,    74,    75,    76,    77,    78,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,    92,    93,    94,    95,    96,    97,     0,     0,     0,
       0,     0,     0,     0,    73,    74,    75,    76,    77,    78,
      79,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    90,    91,    92,    93,    94,    95,    96,    97,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    73,    74,    75,    76,    77,    78,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,    92,    93,    94,    95,    96,    97,     0,     0,     0,
       0,     0,     0,     0,    73,    74,    75,    76,    77,    78,
      79,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    90,    91,    92,    93,    94,    95,    96,    97,     0,
     409,   410,   411,   412,   413,   414,   415,   416,   417,   418,
     419,   420,   421,   422,   423,   424,   425,   426,   427,   428,
     429,     0,     0,     0,    73,    74,    75,    76,    77,    78,
      79,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    90,    91,    92,    93,    94,    95,    96,    97,   633,
     634,   635,     0,     0,     0,     0,   636,     0,     0,     0,
       0,     0,     0,     0,   637,     0,     0,     0,   638,   639,
       0,   640,   629,     0,     0,     0,     0,   641,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   642,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   643,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   644,     0,   645,     0,   646,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     647,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   648,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     649,   650,   651,     0,     0,     0,     0,     0,     0,     0,
     652,   653,   409,   410,   411,   412,   413,   414,   415,   416,
     417,   418,   419,   420,   421,   422,   423,   424,   425,   426,
     427,   428,   429,     0,     0,     0,     0,     0,     0,     0,
       0,   520,   409,   410,   411,   412,   413,   414,   415,   416,
     417,   418,   419,   420,   421,   422,   423,   424,   425,   426,
     427,   428,   429,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,   421,   422,   423,   424,   425,
     426,   427,   428,   429,   521,     0,     0,     0,     0,     0,
       0,     0,     0,   576,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,   421,   422,   423,   424,   425,
     426,   427,   428,   429,   631,     0,     0,     0,     0,     0,
     577,   409,   410,   411,   412,   413,   414,   415,   416,   417,
     418,   419,   420,   421,   422,   423,   424,   425,   426,   427,
     428,   429,     0,     0,   532,   409,   410,   411,   412,   413,
     414,   415,   416,   417,   418,   419,   420,   421,   422,   423,
     424,   425,   426,   427,   428,   429,     0,     0,   656,   409,
     410,   411,   412,   413,   414,   415,   416,   417,   418,   419,
     420,   421,   422,   423,   424,   425,   426,   427,   428,   429,
       0,     0,   742,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,   421,   422,   423,   424,   425,
     426,   427,   428,   429,   411,   412,   413,   414,   415,   416,
     417,   418,   419,   420,   421,   422,   423,   424,   425,   426,
     427,   428,   429
};

#define yypact_value_is_default(yystate) \
  ((yystate) == (-547))

#define yytable_value_is_error(yytable_value) \
  YYID (0)

static const yytype_int16 yycheck[] =
{
       0,     1,     8,     9,    25,    11,     1,   360,   279,    15,
     109,   196,   167,   168,   169,   447,    22,   328,   109,     8,
       9,   109,   151,   109,     8,     9,   226,    41,    65,   132,
      10,    41,    15,    70,    41,   301,    41,   140,    28,    41,
      20,    83,    41,    41,   172,    41,    24,   420,   102,    41,
      41,    90,   120,    41,    58,    90,   352,    81,    64,    40,
     114,    62,   121,   173,    79,    71,   173,    40,    22,   174,
      40,     4,    42,   112,    30,    64,    83,    15,    16,    17,
      64,   120,    71,   176,   100,    20,   123,    71,    20,   153,
     636,    40,   204,    73,   158,    83,     5,   207,    91,   121,
     207,    89,   207,    12,   168,    45,   652,   653,    90,   109,
     459,   104,   540,   212,   463,   108,    55,   168,   111,    94,
     249,   212,   550,   551,   212,   254,   212,   133,   121,   258,
     136,   146,   235,    66,    90,   525,   239,    77,    73,   207,
      99,    73,   495,    53,   137,   330,   200,   165,   207,   129,
     174,     4,     5,     6,   160,     8,     9,    10,   164,    12,
      13,    14,   143,   163,   206,   166,   265,   540,   207,   175,
     143,   160,   207,   179,   302,   164,   160,   550,   551,   133,
     164,   187,   136,   173,   122,   207,   174,   483,   166,   193,
     196,   207,   175,   207,   143,   134,   196,   207,   187,   206,
     237,   181,   207,   187,   210,   207,   116,   196,   207,   207,
     216,   207,   212,   158,   251,   207,   207,     0,   206,    40,
     107,   210,   174,   168,   128,   179,    41,   216,   618,   619,
     236,    41,    59,   158,   240,   241,   103,   243,   244,   245,
     150,    89,     4,   168,   373,   374,   674,   675,   676,   677,
     678,   679,   680,   681,   243,   244,   245,    19,   112,   243,
     244,   245,     4,   366,   112,   265,   120,   370,     4,    15,
      16,    17,   120,   103,    43,    43,   169,    46,    46,   279,
     101,   281,   236,   554,   279,   470,   240,   241,   325,    40,
     475,   135,   145,   168,   494,    41,   296,   297,   298,   336,
     123,   674,   675,   676,   677,   678,   679,   680,   681,   130,
      79,    79,   349,   350,   580,   581,   582,    92,   746,   140,
      90,   182,    42,    74,   330,    89,   332,   188,   334,   190,
     330,   430,   764,    42,     8,   330,    10,    11,   207,   430,
     201,   330,   430,   332,   430,   334,    93,   658,    91,   178,
     179,   704,   663,   158,   360,   106,   183,   184,   185,   196,
     187,   104,   189,   132,   132,   108,   112,    91,   111,   801,
     197,   198,   199,   746,   120,   202,   122,   152,   121,   154,
     104,    89,   133,     3,   108,    67,    68,   111,   196,   395,
     396,   112,   143,   205,   137,     8,   396,    10,    11,   146,
     203,   396,   408,   476,   477,   180,   177,    20,   155,   156,
      40,   432,   175,   137,    54,   167,   167,   118,    19,   157,
     575,   421,    28,    29,    30,   431,    20,   612,   207,   102,
     430,    32,    33,    34,    35,    36,    37,    38,   173,    86,
     446,   395,   396,    63,    64,    14,   207,   453,   454,   455,
     456,   457,   166,    90,   408,   461,    34,    35,    36,    37,
      38,   467,   207,     5,   470,   471,    40,   456,    24,   475,
     470,    91,   456,   479,    94,   475,     4,   431,   467,     5,
      61,   470,   471,   146,     3,    40,   475,   165,    42,   495,
     479,   497,   446,    40,    13,    14,    19,   497,    42,   453,
     454,   455,    40,   457,    89,   125,    42,   461,   497,    32,
      33,    34,    35,    36,    37,    38,    45,    46,   296,   297,
     298,   141,   207,    24,   144,    24,    24,   712,    32,    33,
      34,    35,    36,    37,    38,    64,   118,    62,   158,   159,
     207,   541,   296,   297,   298,    74,   313,   314,   315,    54,
      79,   822,   118,    86,   554,   233,   234,   177,    40,   554,
      54,   660,   207,   207,   242,     4,   186,    96,   166,   247,
      40,   207,   192,    41,    41,    94,    95,   106,   149,   170,
     207,   174,   260,     3,    40,   205,    27,    28,    29,    30,
      31,    32,    33,    34,    35,    36,    37,    38,   276,   277,
     278,   279,   280,    17,   133,   207,   612,   126,     4,   112,
       4,    24,   612,    24,   143,    34,    35,    36,    37,    38,
      40,   299,   124,   612,    41,   144,   145,    28,    29,    30,
      31,    32,    33,    34,    35,    36,    37,    38,   167,    29,
      30,    31,    32,    33,    34,    35,    36,    37,    38,   327,
     108,   657,    41,    41,     5,   661,    24,    40,    38,    41,
     660,    59,   181,   182,   183,   184,   185,   186,   187,   188,
     189,   190,   191,   192,   193,   194,   195,   196,   197,   198,
     199,   200,   201,   202,   203,   204,   205,   365,     4,     4,
     166,   369,    47,   109,    40,    40,   127,    40,   704,    40,
       4,   707,   708,   657,    41,    82,   712,   661,   386,   142,
     388,   389,   712,    40,   392,     5,    97,     5,     5,   708,
       5,    40,    24,   712,    38,    24,   195,    10,    10,   207,
      54,   409,   410,    98,   412,   413,   414,   415,   416,   417,
     418,   419,    10,    41,    41,   423,   424,   425,   426,   427,
     428,   429,    41,   707,    24,   109,     4,     4,     4,   152,
     766,   181,   182,   183,   184,   185,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
     200,   201,   202,   203,   204,   205,   464,     4,     8,     5,
     796,     5,     5,     5,     5,   207,   796,   768,   769,   770,
     771,   772,   773,   774,   775,   776,    41,   796,     5,    24,
       4,   163,   766,     5,    24,   194,    24,   534,   541,    23,
     453,   657,   707,   820,   502,   590,   646,   793,   506,   766,
     357,   650,   171,   711,   512,   579,   514,   498,   516,   688,
     330,   519,   101,   521,   325,   152,    18,   211,   482,   216,
     350,   458,    -1,   531,   241,   533,   446,     3,     4,     5,
       6,   539,     8,     9,    10,    -1,    12,    13,    14,    40,
      -1,    -1,    -1,    -1,    20,    46,   554,    -1,    49,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    32,    33,    34,    60,
      -1,    -1,    -1,    64,    40,    41,    -1,    -1,    -1,    45,
      71,   579,    -1,    74,    75,    76,    -1,    53,    79,    -1,
      56,    -1,    -1,    -1,    85,    -1,    87,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    96,    -1,    -1,    -1,    -1,
      -1,    77,    -1,    -1,    -1,   106,    -1,    -1,    -1,    -1,
      86,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   627,
      -1,   629,    -1,   631,    -1,    -1,    -1,   128,    -1,    -1,
      -1,   132,   133,    -1,   110,   136,    -1,   138,   139,    -1,
      -1,    -1,   143,    -1,    -1,   146,   147,    -1,    -1,    -1,
     151,   659,    -1,    -1,   662,    -1,    -1,    -1,   666,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   167,    -1,    -1,   145,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   164,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   709,    -1,     3,    -1,   181,   182,   183,   184,   185,
     186,   187,   188,   189,   190,   191,   192,   193,   194,   195,
     196,   197,   198,   199,   200,   201,   202,   203,   204,   205,
      -1,     3,    -1,    -1,    -1,   743,    -1,    -1,    -1,    -1,
      40,    -1,    -1,    -1,    -1,    -1,    18,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    37,    38,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   782,    47,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   792,   793,    -1,    -1,    -1,    -1,
      -1,    -1,     3,     4,     5,     6,    -1,     8,     9,    10,
      -1,    12,    13,    14,    -1,    -1,    -1,    -1,    -1,    20,
      -1,    -1,    -1,    -1,   822,   823,    -1,    -1,    -1,    -1,
      -1,    32,    33,    34,    -1,     3,     4,     5,     6,    40,
       8,     9,    10,    -1,    12,    13,    14,    -1,    -1,    -1,
      -1,    -1,    20,   143,    -1,    56,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    32,    33,    -1,    -1,    -1,    -1,
      -1,    -1,    40,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    86,    -1,    -1,    56,    -1,
      -1,   181,   182,   183,   184,   185,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
     200,   201,   202,   203,   204,   205,    -1,    -1,    86,   181,
     182,   183,   184,   185,   186,   187,   188,   189,   190,   191,
     192,   193,   194,   195,   196,   197,   198,   199,   200,   201,
     202,   203,   204,   205,   145,    -1,    -1,     3,    18,    19,
      20,    21,    22,    23,    24,    25,    26,    27,    28,    29,
      30,    31,    32,    33,    34,    35,    36,    37,    38,    -1,
      -1,    -1,    -1,    -1,    -1,   143,    -1,   145,    34,    -1,
     181,   182,   183,   184,   185,   186,   187,   188,   189,   190,
     191,   192,   193,   194,   195,   196,   197,   198,   199,   200,
     201,   202,   203,   204,   205,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   181,   182,   183,   184,   185,   186,   187,
     188,   189,   190,   191,   192,   193,   194,   195,   196,   197,
     198,   199,   200,   201,   202,   203,   204,   205,     3,     4,
       5,     6,     7,     8,     9,    10,    -1,    12,    13,    14,
      -1,    -1,    -1,    -1,    -1,    20,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    32,    33,    -1,
      -1,     3,     4,     5,     6,    40,     8,     9,    10,    -1,
      12,    13,    14,    -1,    -1,    -1,    -1,    -1,    20,    -1,
     160,    56,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      32,    33,    -1,    -1,    -1,    -1,    -1,    -1,    40,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    86,    -1,    -1,    56,   181,   182,   183,   184,   185,
     186,   187,   188,   189,   190,   191,   192,   193,   194,   195,
     196,   197,   198,   199,   200,   201,   202,   203,   204,   205,
      -1,    -1,    -1,    -1,    86,    -1,    -1,    -1,    90,     3,
       4,     5,     6,    -1,     8,     9,    10,    -1,    12,    13,
      14,    -1,    -1,    -1,    -1,    -1,    20,    -1,    -1,    -1,
     145,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    32,    33,
      -1,    -1,    -1,    -1,    -1,    -1,    40,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    56,   145,    -1,    -1,   181,   182,   183,   184,
     185,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,   200,   201,   202,   203,   204,
     205,    -1,    86,    -1,    -1,    -1,    90,    -1,    -1,   181,
     182,   183,   184,   185,   186,   187,   188,   189,   190,   191,
     192,   193,   194,   195,   196,   197,   198,   199,   200,   201,
     202,   203,   204,   205,    -1,    -1,    -1,    -1,    -1,     3,
       4,     5,     6,    -1,     8,     9,    10,    -1,    12,    13,
      14,    -1,    -1,    -1,    -1,    -1,    20,    -1,    -1,    -1,
      -1,   145,    -1,    -1,    -1,    -1,    -1,    -1,    32,    33,
      -1,    -1,    -1,    -1,    -1,    -1,    40,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    56,    -1,    -1,    -1,    -1,   181,   182,   183,
     184,   185,   186,   187,   188,   189,   190,   191,   192,   193,
     194,   195,   196,   197,   198,   199,   200,   201,   202,   203,
     204,   205,    86,    -1,    -1,    -1,    90,     3,     4,     5,
       6,    -1,     8,     9,    10,    -1,    12,    13,    14,    -1,
      -1,    -1,    -1,    -1,    20,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    32,    33,    -1,    -1,
       3,     4,     5,     6,    40,     8,     9,    10,    -1,    12,
      13,    14,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      56,   145,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    32,
      33,     3,    -1,    -1,    -1,    -1,    -1,    40,    -1,    -1,
      -1,    13,    14,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      86,    -1,    -1,    56,    -1,    -1,    -1,   181,   182,   183,
     184,   185,   186,   187,   188,   189,   190,   191,   192,   193,
     194,   195,   196,   197,   198,   199,   200,   201,   202,   203,
     204,   205,    -1,    86,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,     3,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   145,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    94,    95,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    40,     3,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   145,    -1,    -1,   181,   182,   183,   184,   185,
     186,   187,   188,   189,   190,   191,   192,   193,   194,   195,
     196,   197,   198,   199,   200,   201,   202,   203,   204,   205,
      41,     3,   144,   145,    -1,    -1,    47,    -1,   181,   182,
     183,   184,   185,   186,   187,   188,   189,   190,   191,   192,
     193,   194,   195,   196,   197,   198,   199,   200,   201,   202,
     203,   204,   205,     3,    -1,    -1,    -1,    -1,    40,   181,
     182,   183,   184,   185,   186,   187,   188,   189,   190,   191,
     192,   193,   194,   195,   196,   197,   198,   199,   200,   201,
     202,   203,   204,   205,   143,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    47,    80,    -1,
      -1,    -1,    -1,     3,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   181,   182,   183,   184,   185,   186,   187,   188,
     189,   190,   191,   192,   193,   194,   195,   196,   197,   198,
     199,   200,   201,   202,   203,   204,   205,    47,    -1,    -1,
      -1,    -1,    -1,    -1,     3,    -1,    -1,    -1,    -1,    -1,
     181,   182,   183,   184,   185,   186,   187,   188,   189,   190,
     191,   192,   193,   194,   195,   196,   197,   198,   199,   200,
     201,   202,   203,   204,   205,    34,     3,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   181,
     182,   183,   184,   185,   186,   187,   188,   189,   190,   191,
     192,   193,   194,   195,   196,   197,   198,   199,   200,   201,
     202,   203,   204,   205,     3,    -1,    -1,    -1,    -1,    -1,
      -1,   181,   182,   183,   184,   185,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
     200,   201,   202,   203,   204,   205,     3,    18,    19,    20,
      21,    22,    23,    24,    25,    26,    27,    28,    29,    30,
      31,    32,    33,    34,    35,    36,    37,    38,    -1,    -1,
      -1,   181,   182,   183,   184,   185,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
     200,   201,   202,   203,   204,   205,     3,    -1,    -1,    -1,
      -1,    -1,   129,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    90,
      -1,    -1,   181,   182,   183,   184,   185,   186,   187,   188,
     189,   190,   191,   192,   193,   194,   195,   196,   197,   198,
     199,   200,   201,   202,   203,   204,   205,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   181,   182,   183,   184,   185,   186,
     187,   188,   189,   190,   191,   192,   193,   194,   195,   196,
     197,   198,   199,   200,   201,   202,   203,   204,   205,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   181,   182,   183,   184,   185,   186,   187,   188,
     189,   190,   191,   192,   193,   194,   195,   196,   197,   198,
     199,   200,   201,   202,   203,   204,   205,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   181,   182,   183,   184,   185,   186,
     187,   188,   189,   190,   191,   192,   193,   194,   195,   196,
     197,   198,   199,   200,   201,   202,   203,   204,   205,    -1,
      18,    19,    20,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    31,    32,    33,    34,    35,    36,    37,
      38,    -1,    -1,    -1,   181,   182,   183,   184,   185,   186,
     187,   188,   189,   190,   191,   192,   193,   194,   195,   196,
     197,   198,   199,   200,   201,   202,   203,   204,   205,    50,
      51,    52,    -1,    -1,    -1,    -1,    57,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    65,    -1,    -1,    -1,    69,    70,
      -1,    72,    90,    -1,    -1,    -1,    -1,    78,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    88,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   105,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   115,    -1,   117,    -1,   119,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     131,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   148,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     161,   162,   163,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     171,   172,    18,    19,    20,    21,    22,    23,    24,    25,
      26,    27,    28,    29,    30,    31,    32,    33,    34,    35,
      36,    37,    38,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    47,    18,    19,    20,    21,    22,    23,    24,    25,
      26,    27,    28,    29,    30,    31,    32,    33,    34,    35,
      36,    37,    38,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    90,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    48,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    90,    -1,    -1,    -1,    -1,    -1,
      75,    18,    19,    20,    21,    22,    23,    24,    25,    26,
      27,    28,    29,    30,    31,    32,    33,    34,    35,    36,
      37,    38,    -1,    -1,    41,    18,    19,    20,    21,    22,
      23,    24,    25,    26,    27,    28,    29,    30,    31,    32,
      33,    34,    35,    36,    37,    38,    -1,    -1,    41,    18,
      19,    20,    21,    22,    23,    24,    25,    26,    27,    28,
      29,    30,    31,    32,    33,    34,    35,    36,    37,    38,
      -1,    -1,    41,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    20,    21,    22,    23,    24,    25,
      26,    27,    28,    29,    30,    31,    32,    33,    34,    35,
      36,    37,    38
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,    40,    46,    49,    60,    64,    71,    74,    75,    76,
      79,    85,    87,    96,   106,   128,   132,   133,   136,   138,
     139,   143,   146,   147,   151,   167,   209,   210,   211,   229,
     230,   233,   249,   252,   254,   259,   260,   261,   262,   263,
     264,   293,   296,   303,   308,   310,   312,   315,   319,   320,
     321,   322,   325,   330,   332,   335,   340,   343,   344,   345,
     351,   260,   261,   153,   158,   168,   176,   317,   317,   158,
     168,    90,     3,   181,   182,   183,   184,   185,   186,   187,
     188,   189,   190,   191,   192,   193,   194,   195,   196,   197,
     198,   199,   200,   201,   202,   203,   204,   205,   289,   360,
     363,   289,   158,   168,     3,   333,   362,   363,   204,   295,
      45,    46,    64,    74,    79,    96,   106,   133,   143,   167,
     326,   327,   333,   168,   326,   317,    94,   323,    99,   267,
       3,    13,    14,    94,    95,   126,   144,   145,   336,   337,
     359,   363,     3,    63,    64,    91,    94,   125,   141,   144,
     158,   159,   186,   192,   205,   224,   299,   302,   361,   165,
     267,     0,    83,   206,   107,   174,   253,    15,    16,    17,
     122,   278,   279,    89,   275,   128,    41,    41,    93,   146,
     155,   156,   352,   289,     4,   306,   103,   234,   304,   305,
     306,   289,     4,   301,   301,   103,   250,   306,   309,   169,
     341,   229,   230,   252,   259,   294,   135,   328,   168,   123,
     121,   207,    90,   306,   313,   314,   121,   331,   178,   179,
     324,    40,   101,   130,   140,   268,   269,   270,    45,    77,
     284,    24,   166,   338,   338,   359,    42,    89,   311,   359,
      42,   207,   338,    30,    90,   158,    28,   173,   300,   196,
     300,    89,   298,   311,   196,   112,   297,   205,   203,   177,
      40,   175,   318,   289,   211,   289,     3,     4,     5,     6,
       8,     9,    10,    12,    13,    14,    20,    32,    33,    40,
      56,    86,   145,   191,   213,   214,   215,   217,   219,   224,
     225,   226,   260,   359,   360,   363,   284,   284,   284,    54,
     112,   120,   266,   275,   167,   333,   353,   354,   359,   118,
     157,    67,    68,    43,    46,    79,   132,   346,   347,   102,
     114,   200,   316,    20,   289,   207,   102,   173,   265,    86,
      40,   251,   260,   288,   289,   290,   207,    14,   339,   342,
      34,   329,   360,   327,   229,   230,   252,   259,   334,   166,
     207,   329,    90,     5,   271,    40,   100,   207,   270,    34,
     217,   285,   286,   217,   217,   338,   359,   306,    24,   338,
     359,   337,   217,   289,   289,   289,     4,   217,   300,    66,
     300,     5,   300,    34,    41,    45,    53,    77,   110,   164,
     212,   217,   228,   276,    61,   146,    40,   255,   259,   217,
     217,   217,   212,   217,   260,   217,   220,   260,    42,    18,
      19,    20,    21,    22,    23,    24,    25,    26,    27,    28,
      29,    30,    31,    32,    33,    34,    35,    36,    37,    38,
      40,    42,   143,   260,   263,   264,   263,   263,   217,   280,
     281,     5,    12,   273,   273,   275,   207,    24,    24,    24,
     118,   118,    62,   348,   348,   348,   166,   348,   207,    54,
      86,    40,   305,    54,     7,   217,   253,   260,   288,   290,
     207,    47,   360,    91,   104,   108,   111,   137,   291,    47,
     360,   306,   207,   166,   306,   313,   309,    41,   207,    92,
     152,   154,   180,   272,   269,    47,   362,    90,   207,   266,
     274,   217,   338,     4,   307,   217,   338,   300,   300,    40,
     207,    41,    90,   217,    90,   217,    90,   217,    41,   207,
      47,    90,   217,    41,   149,   231,   232,   359,   256,   359,
     170,   207,    41,   174,   221,   222,   359,   217,   217,    28,
      29,    30,   217,   217,   217,   217,   217,   217,   217,   217,
      32,    33,   215,   216,    40,   218,   260,     8,    10,    11,
      20,   217,   217,   217,   217,   217,   217,   217,   227,   229,
     230,   252,   259,    34,   359,   267,    48,    75,   282,   207,
     120,   207,   112,   354,   214,     4,     4,    24,    24,   237,
     359,   359,   359,   289,   359,   132,   347,   307,   129,   235,
     236,   237,   307,   217,    41,   288,   360,   124,   292,   288,
     292,   292,   108,   360,   339,   309,    41,   362,    80,   287,
     288,   285,   217,   217,    41,     5,   217,    90,   217,    90,
     217,    90,   217,    50,    51,    52,    57,    65,    69,    70,
      72,    78,    88,   105,   115,   117,   119,   131,   148,   161,
     162,   163,   171,   172,   238,   217,    41,   207,   265,    24,
      41,   207,    40,   257,   217,   217,    81,   222,   223,   217,
     216,   218,   216,   216,    19,    32,    33,    34,    35,    36,
      37,    38,   212,   217,     8,    10,    11,    41,   284,   281,
     273,   273,   273,    59,   355,     4,     4,   238,    79,   146,
     350,    55,   134,   349,   166,   109,    41,   207,    47,   121,
     288,   265,   207,   265,   217,   217,   217,    40,   243,   243,
      40,   239,   127,   241,    40,   240,   239,    40,   242,   242,
     243,   243,    41,   232,   253,   217,   259,   359,   217,   258,
     207,   253,    41,   160,   217,    82,    19,   216,   216,   216,
     216,   216,   216,   216,   216,    41,   286,     4,   142,   356,
     244,    20,    73,    20,    73,   362,    40,    59,   183,   184,
     185,   187,   189,   197,   198,   199,   202,   246,   247,   236,
     360,   217,   121,   274,   288,    97,   277,     5,     5,     5,
       5,    41,   207,    40,   217,   216,    90,    24,   195,    10,
      20,    73,   129,   181,   245,    10,    10,   214,   256,    24,
     248,   248,   248,   248,   248,   248,   248,   248,   248,   248,
     207,   217,    54,    98,   283,    41,    41,   207,    41,    41,
     217,   258,    53,   116,   150,    24,    10,   214,   109,    41,
       4,     4,   152,     4,     4,     5,     5,     5,     5,     8,
     247,   212,   217,     5,    41,   182,   188,   190,   201,   357,
      41,    58,   193,   358,    24,    24,     5,     4,   194,    24,
       5
};

#define yyerrok		(yyerrstatus = 0)
#define yyclearin	(yychar = YYEMPTY)
#define YYEMPTY		(-2)
#define YYEOF		0

#define YYACCEPT	goto yyacceptlab
#define YYABORT		goto yyabortlab
#define YYERROR		goto yyerrorlab


/* Like YYERROR except do call yyerror.  This remains here temporarily
   to ease the transition to the new meaning of YYERROR, for GCC.
   Once GCC version 2 has supplanted version 1, this can go.  However,
   YYFAIL appears to be in use.  Nevertheless, it is formally deprecated
   in Bison 2.4.2's NEWS entry, where a plan to phase it out is
   discussed.  */

#define YYFAIL		goto yyerrlab
#if defined YYFAIL
  /* This is here to suppress warnings from the GCC cpp's
     -Wunused-macros.  Normally we don't worry about that warning, but
     some users do, and we want to make it easy for users to remove
     YYFAIL uses, which will produce warnings from Bison 2.5.  */
#endif

#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)					\
do								\
  if (yychar == YYEMPTY && yylen == 1)				\
    {								\
      yychar = (Token);						\
      yylval = (Value);						\
      YYPOPSTACK (1);						\
      goto yybackup;						\
    }								\
  else								\
    {								\
      yyerror (&yylloc, result, YY_("syntax error: cannot back up")); \
      YYERROR;							\
    }								\
while (YYID (0))


#define YYTERROR	1
#define YYERRCODE	256


/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#define YYRHSLOC(Rhs, K) ((Rhs)[K])
#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)				\
    do									\
      if (YYID (N))                                                    \
	{								\
	  (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;	\
	  (Current).first_column = YYRHSLOC (Rhs, 1).first_column;	\
	  (Current).last_line    = YYRHSLOC (Rhs, N).last_line;		\
	  (Current).last_column  = YYRHSLOC (Rhs, N).last_column;	\
	}								\
      else								\
	{								\
	  (Current).first_line   = (Current).last_line   =		\
	    YYRHSLOC (Rhs, 0).last_line;				\
	  (Current).first_column = (Current).last_column =		\
	    YYRHSLOC (Rhs, 0).last_column;				\
	}								\
    while (YYID (0))
#endif


/* YY_LOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

#ifndef YY_LOCATION_PRINT
# if defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL
#  define YY_LOCATION_PRINT(File, Loc)			\
     fprintf (File, "%d.%d-%d.%d",			\
	      (Loc).first_line, (Loc).first_column,	\
	      (Loc).last_line,  (Loc).last_column)
# else
#  define YY_LOCATION_PRINT(File, Loc) ((void) 0)
# endif
#endif


/* YYLEX -- calling `yylex' with the right arguments.  */

#ifdef YYLEX_PARAM
# define YYLEX yylex (&yylval, &yylloc, YYLEX_PARAM)
#else
# define YYLEX yylex (&yylval, &yylloc)
#endif

/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)			\
do {						\
  if (yydebug)					\
    YYFPRINTF Args;				\
} while (YYID (0))

# define YY_SYMBOL_PRINT(Title, Type, Value, Location)			  \
do {									  \
  if (yydebug)								  \
    {									  \
      YYFPRINTF (stderr, "%s ", Title);					  \
      yy_symbol_print (stderr,						  \
		  Type, Value, Location, result); \
      YYFPRINTF (stderr, "\n");						  \
    }									  \
} while (YYID (0))


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_value_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, ParseResult* result)
#else
static void
yy_symbol_value_print (yyoutput, yytype, yyvaluep, yylocationp, result)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
    YYLTYPE const * const yylocationp;
    ParseResult* result;
#endif
{
  if (!yyvaluep)
    return;
  YYUSE (yylocationp);
  YYUSE (result);
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyoutput, yytoknum[yytype], *yyvaluep);
# else
  YYUSE (yyoutput);
# endif
  switch (yytype)
    {
      default:
	break;
    }
}


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, ParseResult* result)
#else
static void
yy_symbol_print (yyoutput, yytype, yyvaluep, yylocationp, result)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
    YYLTYPE const * const yylocationp;
    ParseResult* result;
#endif
{
  if (yytype < YYNTOKENS)
    YYFPRINTF (yyoutput, "token %s (", yytname[yytype]);
  else
    YYFPRINTF (yyoutput, "nterm %s (", yytname[yytype]);

  YY_LOCATION_PRINT (yyoutput, *yylocationp);
  YYFPRINTF (yyoutput, ": ");
  yy_symbol_value_print (yyoutput, yytype, yyvaluep, yylocationp, result);
  YYFPRINTF (yyoutput, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_stack_print (yytype_int16 *yybottom, yytype_int16 *yytop)
#else
static void
yy_stack_print (yybottom, yytop)
    yytype_int16 *yybottom;
    yytype_int16 *yytop;
#endif
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)				\
do {								\
  if (yydebug)							\
    yy_stack_print ((Bottom), (Top));				\
} while (YYID (0))


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_reduce_print (YYSTYPE *yyvsp, YYLTYPE *yylsp, int yyrule, ParseResult* result)
#else
static void
yy_reduce_print (yyvsp, yylsp, yyrule, result)
    YYSTYPE *yyvsp;
    YYLTYPE *yylsp;
    int yyrule;
    ParseResult* result;
#endif
{
  int yynrhs = yyr2[yyrule];
  int yyi;
  unsigned long int yylno = yyrline[yyrule];
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %lu):\n",
	     yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr, yyrhs[yyprhs[yyrule] + yyi],
		       &(yyvsp[(yyi + 1) - (yynrhs)])
		       , &(yylsp[(yyi + 1) - (yynrhs)])		       , result);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)		\
do {					\
  if (yydebug)				\
    yy_reduce_print (yyvsp, yylsp, Rule, result); \
} while (YYID (0))

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef	YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen strlen
#  else
/* Return the length of YYSTR.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static YYSIZE_T
yystrlen (const char *yystr)
#else
static YYSIZE_T
yystrlen (yystr)
    const char *yystr;
#endif
{
  YYSIZE_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static char *
yystpcpy (char *yydest, const char *yysrc)
#else
static char *
yystpcpy (yydest, yysrc)
    char *yydest;
    const char *yysrc;
#endif
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYSIZE_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYSIZE_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
	switch (*++yyp)
	  {
	  case '\'':
	  case ',':
	    goto do_not_strip_quotes;

	  case '\\':
	    if (*++yyp != '\\')
	      goto do_not_strip_quotes;
	    /* Fall through.  */
	  default:
	    if (yyres)
	      yyres[yyn] = *yyp;
	    yyn++;
	    break;

	  case '"':
	    if (yyres)
	      yyres[yyn] = '\0';
	    return yyn;
	  }
    do_not_strip_quotes: ;
    }

  if (! yyres)
    return yystrlen (yystr);

  return yystpcpy (yyres, yystr) - yyres;
}
# endif

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return 1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return 2 if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYSIZE_T *yymsg_alloc, char **yymsg,
                yytype_int16 *yyssp, int yytoken)
{
  YYSIZE_T yysize0 = yytnamerr (0, yytname[yytoken]);
  YYSIZE_T yysize = yysize0;
  YYSIZE_T yysize1;
  enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
  /* Internationalized format string. */
  const char *yyformat = 0;
  /* Arguments of yyformat. */
  char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
  /* Number of reported tokens (one for the "unexpected", one per
     "expected"). */
  int yycount = 0;

  /* There are many possibilities here to consider:
     - Assume YYFAIL is not used.  It's too flawed to consider.  See
       <http://lists.gnu.org/archive/html/bison-patches/2009-12/msg00024.html>
       for details.  YYERROR is fine as it does not invoke this
       function.
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yytoken != YYEMPTY)
    {
      int yyn = yypact[*yyssp];
      yyarg[yycount++] = yytname[yytoken];
      if (!yypact_value_is_default (yyn))
        {
          /* Start YYX at -YYN if negative to avoid negative indexes in
             YYCHECK.  In other words, skip the first -YYN actions for
             this state because they are default actions.  */
          int yyxbegin = yyn < 0 ? -yyn : 0;
          /* Stay within bounds of both yycheck and yytname.  */
          int yychecklim = YYLAST - yyn + 1;
          int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
          int yyx;

          for (yyx = yyxbegin; yyx < yyxend; ++yyx)
            if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR
                && !yytable_value_is_error (yytable[yyx + yyn]))
              {
                if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
                  {
                    yycount = 1;
                    yysize = yysize0;
                    break;
                  }
                yyarg[yycount++] = yytname[yyx];
                yysize1 = yysize + yytnamerr (0, yytname[yyx]);
                if (! (yysize <= yysize1
                       && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
                  return 2;
                yysize = yysize1;
              }
        }
    }

  switch (yycount)
    {
# define YYCASE_(N, S)                      \
      case N:                               \
        yyformat = S;                       \
      break
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
# undef YYCASE_
    }

  yysize1 = yysize + yystrlen (yyformat);
  if (! (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
    return 2;
  yysize = yysize1;

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return 1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yyarg[yyi++]);
          yyformat += 2;
        }
      else
        {
          yyp++;
          yyformat++;
        }
  }
  return 0;
}
#endif /* YYERROR_VERBOSE */

/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep, YYLTYPE *yylocationp, ParseResult* result)
#else
static void
yydestruct (yymsg, yytype, yyvaluep, yylocationp, result)
    const char *yymsg;
    int yytype;
    YYSTYPE *yyvaluep;
    YYLTYPE *yylocationp;
    ParseResult* result;
#endif
{
  YYUSE (yyvaluep);
  YYUSE (yylocationp);
  YYUSE (result);

  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  switch (yytype)
    {
      case 3: /* "NAME" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 4: /* "STRING" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 5: /* "INTNUM" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 6: /* "DATE_VALUE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 7: /* "HINT_VALUE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 8: /* "BOOL" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 9: /* "APPROXNUM" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 10: /* "NULLX" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 11: /* "UNKNOWN" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 12: /* "QUESTIONMARK" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 13: /* "SYSTEM_VARIABLE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 14: /* "TEMP_VARIABLE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 209: /* "sql_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 210: /* "stmt_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 211: /* "stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 212: /* "expr_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 213: /* "column_ref" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 214: /* "expr_const" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 215: /* "simple_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 216: /* "arith_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 217: /* "expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 218: /* "in_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 219: /* "case_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 220: /* "case_arg" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 221: /* "when_clause_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 222: /* "when_clause" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 223: /* "case_default" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 224: /* "func_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 225: /* "when_func" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 226: /* "when_func_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 227: /* "when_func_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 228: /* "distinct_or_all" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 229: /* "delete_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 230: /* "update_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 231: /* "update_asgn_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 232: /* "update_asgn_factor" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 233: /* "create_table_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 234: /* "opt_if_not_exists" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 235: /* "table_element_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 236: /* "table_element" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 237: /* "column_definition" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 238: /* "data_type" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 239: /* "opt_decimal" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 240: /* "opt_float" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 241: /* "opt_precision" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 242: /* "opt_time_precision" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 243: /* "opt_char_length" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 244: /* "opt_column_attribute_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 245: /* "column_attribute" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 246: /* "opt_table_option_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 247: /* "table_option" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 248: /* "opt_equal_mark" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 249: /* "drop_table_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 250: /* "opt_if_exists" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 251: /* "table_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 252: /* "insert_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 253: /* "opt_when" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 254: /* "replace_or_insert" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 255: /* "opt_insert_columns" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 256: /* "column_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 257: /* "insert_vals_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 258: /* "insert_vals" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 259: /* "select_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 260: /* "select_with_parens" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 261: /* "select_no_parens" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 262: /* "no_table_select" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 263: /* "select_clause" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 264: /* "simple_select" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 265: /* "opt_where" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 266: /* "select_limit" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 267: /* "opt_hint" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 268: /* "opt_hint_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 269: /* "hint_options" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 270: /* "hint_option" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 271: /* "opt_comma_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 273: /* "limit_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 274: /* "opt_select_limit" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 275: /* "opt_for_update" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 276: /* "parameterized_trim" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 277: /* "opt_groupby" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 278: /* "opt_order_by" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 279: /* "order_by" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 280: /* "sort_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 281: /* "sort_key" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 282: /* "opt_asc_desc" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 283: /* "opt_having" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 284: /* "opt_distinct" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 285: /* "projection" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 286: /* "select_expr_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 287: /* "from_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 288: /* "table_factor" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 289: /* "relation_factor" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 290: /* "joined_table" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 291: /* "join_type" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 292: /* "join_outer" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 293: /* "explain_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 294: /* "explainable_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 295: /* "opt_verbose" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 296: /* "show_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 297: /* "opt_limit" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 298: /* "opt_for_grant_user" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 300: /* "opt_show_condition" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 301: /* "opt_like_condition" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 303: /* "create_user_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 304: /* "user_specification_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 305: /* "user_specification" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 306: /* "user" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 307: /* "password" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 308: /* "drop_user_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 309: /* "user_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 310: /* "set_password_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 311: /* "opt_for_user" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 312: /* "rename_user_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 313: /* "rename_info" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 314: /* "rename_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 315: /* "lock_user_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 316: /* "lock_spec" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 317: /* "opt_work" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 319: /* "begin_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 320: /* "commit_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 321: /* "rollback_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 322: /* "kill_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 323: /* "opt_is_global" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 324: /* "opt_flag" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 325: /* "grant_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 326: /* "priv_type_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 327: /* "priv_type" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 328: /* "opt_privilege" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 329: /* "priv_level" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 330: /* "revoke_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 331: /* "opt_on_priv_level" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 332: /* "prepare_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 333: /* "stmt_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 334: /* "preparable_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 335: /* "variable_set_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 336: /* "var_and_val_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 337: /* "var_and_val" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 338: /* "to_or_eq" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 339: /* "argument" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 340: /* "execute_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 341: /* "opt_using_args" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 342: /* "argument_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 343: /* "deallocate_prepare_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 344: /* "deallocate_or_drop" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 345: /* "alter_table_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 346: /* "alter_column_actions" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 347: /* "alter_column_action" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 348: /* "opt_column" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 350: /* "alter_column_behavior" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 351: /* "alter_system_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 352: /* "opt_force" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 353: /* "alter_system_actions" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 354: /* "alter_system_action" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 355: /* "opt_comment" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 357: /* "server_type" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 358: /* "opt_cluster_or_address" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 359: /* "column_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 360: /* "relation_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 361: /* "function_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 362: /* "column_label" */

	{destroy_tree((yyvaluep->node));};

	break;

      default:
	break;
    }
}


/* Prevent warnings from -Wmissing-prototypes.  */
#ifdef YYPARSE_PARAM
#if defined __STDC__ || defined __cplusplus
int yyparse (void *YYPARSE_PARAM);
#else
int yyparse ();
#endif
#else /* ! YYPARSE_PARAM */
#if defined __STDC__ || defined __cplusplus
int yyparse (ParseResult* result);
#else
int yyparse ();
#endif
#endif /* ! YYPARSE_PARAM */


/*----------.
| yyparse.  |
`----------*/

#ifdef YYPARSE_PARAM
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (void *YYPARSE_PARAM)
#else
int
yyparse (YYPARSE_PARAM)
    void *YYPARSE_PARAM;
#endif
#else /* ! YYPARSE_PARAM */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (ParseResult* result)
#else
int
yyparse (result)
    ParseResult* result;
#endif
#endif
{
/* The lookahead symbol.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;

/* Location data for the lookahead symbol.  */
YYLTYPE yylloc;

    /* Number of syntax errors so far.  */
    int yynerrs;

    int yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       `yyss': related to states.
       `yyvs': related to semantic values.
       `yyls': related to locations.

       Refer to the stacks thru separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yytype_int16 yyssa[YYINITDEPTH];
    yytype_int16 *yyss;
    yytype_int16 *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    /* The location stack.  */
    YYLTYPE yylsa[YYINITDEPTH];
    YYLTYPE *yyls;
    YYLTYPE *yylsp;

    /* The locations where the error started and ended.  */
    YYLTYPE yyerror_range[3];

    YYSIZE_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;
  YYLTYPE yyloc;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYSIZE_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N), yylsp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yytoken = 0;
  yyss = yyssa;
  yyvs = yyvsa;
  yyls = yylsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY; /* Cause a token to be read.  */

  /* Initialize stack pointers.
     Waste one element of value and location stack
     so that they stay on the same level as the state stack.
     The wasted elements are never initialized.  */
  yyssp = yyss;
  yyvsp = yyvs;
  yylsp = yyls;

#if defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL
  /* Initialize the default location before parsing starts.  */
  yylloc.first_line   = yylloc.last_line   = 1;
  yylloc.first_column = yylloc.last_column = 1;
#endif

  goto yysetstate;

/*------------------------------------------------------------.
| yynewstate -- Push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
 yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;

 yysetstate:
  *yyssp = yystate;

  if (yyss + yystacksize - 1 <= yyssp)
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYSIZE_T yysize = yyssp - yyss + 1;

#ifdef yyoverflow
      {
	/* Give user a chance to reallocate the stack.  Use copies of
	   these so that the &'s don't force the real ones into
	   memory.  */
	YYSTYPE *yyvs1 = yyvs;
	yytype_int16 *yyss1 = yyss;
	YYLTYPE *yyls1 = yyls;

	/* Each stack pointer address is followed by the size of the
	   data in use in that stack, in bytes.  This used to be a
	   conditional around just the two extra args, but that might
	   be undefined if yyoverflow is a macro.  */
	yyoverflow (YY_("memory exhausted"),
		    &yyss1, yysize * sizeof (*yyssp),
		    &yyvs1, yysize * sizeof (*yyvsp),
		    &yyls1, yysize * sizeof (*yylsp),
		    &yystacksize);

	yyls = yyls1;
	yyss = yyss1;
	yyvs = yyvs1;
      }
#else /* no yyoverflow */
# ifndef YYSTACK_RELOCATE
      goto yyexhaustedlab;
# else
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
	goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
	yystacksize = YYMAXDEPTH;

      {
	yytype_int16 *yyss1 = yyss;
	union yyalloc *yyptr =
	  (union yyalloc *) YYSTACK_ALLOC (YYSTACK_BYTES (yystacksize));
	if (! yyptr)
	  goto yyexhaustedlab;
	YYSTACK_RELOCATE (yyss_alloc, yyss);
	YYSTACK_RELOCATE (yyvs_alloc, yyvs);
	YYSTACK_RELOCATE (yyls_alloc, yyls);
#  undef YYSTACK_RELOCATE
	if (yyss1 != yyssa)
	  YYSTACK_FREE (yyss1);
      }
# endif
#endif /* no yyoverflow */

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;
      yylsp = yyls + yysize - 1;

      YYDPRINTF ((stderr, "Stack size increased to %lu\n",
		  (unsigned long int) yystacksize));

      if (yyss + yystacksize - 1 <= yyssp)
	YYABORT;
    }

  YYDPRINTF ((stderr, "Entering state %d\n", yystate));

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;

/*-----------.
| yybackup.  |
`-----------*/
yybackup:

  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid lookahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = YYLEX;
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

  /* Discard the shifted token.  */
  yychar = YYEMPTY;

  yystate = yyn;
  *++yyvsp = yylval;
  *++yylsp = yylloc;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- Do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     `$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];

  /* Default location.  */
  YYLLOC_DEFAULT (yyloc, (yylsp - yylen), yylen);
  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
        case 2:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_STMT_LIST, (yyvsp[(1) - (2)].node));
      result->result_tree_ = (yyval.node);
      YYACCEPT;
    }
    break;

  case 3:

    {
      if ((yyvsp[(3) - (3)].node) != NULL)
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      else
        (yyval.node) = (yyvsp[(1) - (3)].node);
    }
    break;

  case 4:

    {
      (yyval.node) = ((yyvsp[(1) - (1)].node) != NULL) ? (yyvsp[(1) - (1)].node) : NULL;
    }
    break;

  case 5:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 6:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 7:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 8:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 9:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 10:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 11:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 12:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 13:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 14:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 15:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 16:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 17:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 18:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 19:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 20:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 21:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 22:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 23:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 24:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 25:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 26:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 27:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 28:

    {(yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 29:

    {(yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 30:

    { (yyval.node) = NULL; }
    break;

  case 31:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 32:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 33:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 34:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NAME_FIELD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(3) - (3)]).first_column, (yylsp[(3) - (3)]).last_column);
    }
    break;

  case 35:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_STAR);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NAME_FIELD, 2, (yyvsp[(1) - (3)].node), node);
    }
    break;

  case 36:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 37:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 38:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 39:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 40:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 41:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 42:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 43:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 44:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 45:

    { (yyvsp[(3) - (3)].node)->type_ = T_SYSTEM_VARIABLE; (yyval.node) = (yyvsp[(3) - (3)].node); }
    break;

  case 46:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 47:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 48:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 49:

    {
      ParseNode *node = NULL;
      malloc_non_terminal_node(node, result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(2) - (5)].node), (yyvsp[(4) - (5)].node));
      merge_nodes((yyval.node), result->malloc_pool_, T_EXPR_LIST, node);
    }
    break;

  case 50:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      /*
      yyerror(&@1, result, "CASE expression is not supported yet!");
      YYABORT;
      */
    }
    break;

  case 51:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 52:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 53:

    {
    	(yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 54:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_EXISTS, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 55:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 56:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POS, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 57:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NEG, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 58:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_ADD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 59:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MINUS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 60:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MUL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 61:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_DIV, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 62:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_REM, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 63:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POW, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 64:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MOD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 65:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 66:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POS, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 67:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NEG, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 68:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_ADD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 69:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MINUS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 70:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MUL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 71:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_DIV, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 72:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_REM, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 73:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POW, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 74:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MOD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 75:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 76:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LT, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 77:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_EQ, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 78:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_GE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 79:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_GT, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 80:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 81:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LIKE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 82:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT_LIKE, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node)); }
    break;

  case 83:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_AND, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 84:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_OR, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 85:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 86:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 87:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 88:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 89:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 90:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 91:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 92:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_BTW, 3, (yyvsp[(1) - (5)].node), (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    }
    break;

  case 93:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT_BTW, 3, (yyvsp[(1) - (6)].node), (yyvsp[(4) - (6)].node), (yyvsp[(6) - (6)].node));
    }
    break;

  case 94:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IN, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 95:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT_IN, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 96:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_CNN, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 97:

    {
    	(yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 98:

    { merge_nodes((yyval.node), result->malloc_pool_, T_EXPR_LIST, (yyvsp[(2) - (3)].node)); }
    break;

  case 99:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_WHEN_LIST, (yyvsp[(3) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CASE, 3, (yyvsp[(2) - (5)].node), (yyval.node), (yyvsp[(4) - (5)].node));
    }
    break;

  case 100:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 101:

    { (yyval.node) = NULL; }
    break;

  case 102:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 103:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node)); }
    break;

  case 104:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_WHEN, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 105:

    { (yyval.node) = (yyvsp[(2) - (2)].node); }
    break;

  case 106:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_NULL); }
    break;

  case 107:

    {
      if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "count") != 0)
      {
        yyerror(&(yylsp[(1) - (4)]), result, "Only COUNT function can be with '*' parameter!");
        YYABORT;
      }
      else
      {
        ParseNode* node = NULL;
        malloc_terminal_node(node, result->malloc_pool_, T_STAR);
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_COUNT, 1, node);
      }
    }
    break;

  case 108:

    {
      if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "count") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_COUNT, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "sum") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SUM, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "max") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MAX, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "min") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MIN, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "avg") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_AVG, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else
      {
        yyerror(&(yylsp[(1) - (5)]), result, "Wrong system function with 'DISTINCT/ALL'!");
        YYABORT;
      }
    }
    break;

  case 109:

    {
      if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "count") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "COUNT function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_COUNT, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "sum") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "SUM function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SUM, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "max") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "MAX function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MAX, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "min") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "MIN function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MIN, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "avg") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "AVG function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_AVG, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "trim") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "TRIM function syntax error! TRIM don't take %d params", (yyvsp[(3) - (4)].node)->num_child_);
          YYABORT;
        }
        else
        {
          ParseNode* default_type = NULL;
          malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
          default_type->value_ = 0;
          ParseNode* default_operand = NULL;
          malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
          default_operand->str_value_ = " "; /* blank for default */
          default_operand->value_ = strlen(default_operand->str_value_);
          ParseNode *params = NULL;
          malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (4)].node));
          malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (4)].node), params);
        }
      }
      else  /* system function */
      {
        ParseNode *params = NULL;
        merge_nodes(params, result->malloc_pool_, T_EXPR_LIST, (yyvsp[(3) - (4)].node));
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (4)].node), params);
      }
    }
    break;

  case 110:

    {
      if (strcasecmp((yyvsp[(1) - (6)].node)->str_value_, "cast") == 0)
      {
        (yyvsp[(5) - (6)].node)->value_ = (yyvsp[(5) - (6)].node)->type_;
        (yyvsp[(5) - (6)].node)->type_ = T_INT;
        ParseNode *params = NULL;
        malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, (yyvsp[(3) - (6)].node), (yyvsp[(5) - (6)].node));
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (6)].node), params);
      }
      else
      {
        yyerror(&(yylsp[(1) - (6)]), result, "AS support cast function only!");
        YYABORT;
      }
    }
    break;

  case 111:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (4)].node), (yyvsp[(3) - (4)].node));
    }
    break;

  case 112:

    {
      if (strcasecmp((yyvsp[(1) - (3)].node)->str_value_, "now") == 0 ||
          strcasecmp((yyvsp[(1) - (3)].node)->str_value_, "current_time") == 0 ||
          strcasecmp((yyvsp[(1) - (3)].node)->str_value_, "current_timestamp") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CUR_TIME, 1, (yyvsp[(1) - (3)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (3)].node)->str_value_, "strict_current_timestamp") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CUR_TIME_UPS, 1, (yyvsp[(1) - (3)].node));
      }
      else
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 1, (yyvsp[(1) - (3)].node));
      }
      //yyerror(&@1, result, "system/user-define function is not supported yet!");
      //YYABORT;
    }
    break;

  case 113:

    {
      (yyval.node) = (yyvsp[(1) - (4)].node);
      (yyval.node)->children_[0] = (yyvsp[(3) - (4)].node);
    }
    break;

  case 114:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ROW_COUNT, 1, NULL);
    }
    break;

  case 119:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_ALL);
    }
    break;

  case 120:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_DISTINCT);
    }
    break;

  case 121:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_DELETE, 3, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node));
    }
    break;

  case 122:

    {
      ParseNode* assign_list = NULL;
      merge_nodes(assign_list, result->malloc_pool_, T_ASSIGN_LIST, (yyvsp[(5) - (7)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_UPDATE, 5, (yyvsp[(3) - (7)].node), assign_list, (yyvsp[(6) - (7)].node), (yyvsp[(7) - (7)].node), (yyvsp[(2) - (7)].node));
    }
    break;

  case 123:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 124:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 125:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ASSIGN_ITEM, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 126:

    {
      ParseNode *table_elements = NULL;
      ParseNode *table_options = NULL;
      merge_nodes(table_elements, result->malloc_pool_, T_TABLE_ELEMENT_LIST, (yyvsp[(6) - (8)].node));
      merge_nodes(table_options, result->malloc_pool_, T_TABLE_OPTION_LIST, (yyvsp[(8) - (8)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CREATE_TABLE, 4,
              (yyvsp[(3) - (8)].node),                   /* if not exists */
              (yyvsp[(4) - (8)].node),                   /* table name */
              table_elements,       /* columns or primary key */
              table_options         /* table option(s) */
              );
    }
    break;

  case 127:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_IF_NOT_EXISTS); }
    break;

  case 128:

    { (yyval.node) = NULL; }
    break;

  case 129:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 130:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 131:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 132:

    {
      ParseNode* col_list= NULL;
      merge_nodes(col_list, result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(4) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PRIMARY_KEY, 1, col_list);
    }
    break;

  case 133:

    {
      ParseNode *attributes = NULL;
      merge_nodes(attributes, result->malloc_pool_, T_COLUMN_ATTRIBUTES, (yyvsp[(3) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_DEFINITION, 3, (yyvsp[(1) - (3)].node), (yyvsp[(2) - (3)].node), attributes);
    }
    break;

  case 134:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER ); }
    break;

  case 135:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); }
    break;

  case 136:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); }
    break;

  case 137:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); }
    break;

  case 138:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); }
    break;

  case 139:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL);
      else
        merge_nodes((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL, (yyvsp[(2) - (2)].node));
      yyerror(&(yylsp[(1) - (2)]), result, "DECIMAL type is not supported");
      YYABORT;
    }
    break;

  case 140:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL);
      else
        merge_nodes((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL, (yyvsp[(2) - (2)].node));
      yyerror(&(yylsp[(1) - (2)]), result, "NUMERIC type is not supported");
      YYABORT;
    }
    break;

  case 141:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_BOOLEAN ); }
    break;

  case 142:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_FLOAT, 1, (yyvsp[(2) - (2)].node)); }
    break;

  case 143:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DOUBLE); }
    break;

  case 144:

    {
      (void)((yyvsp[(2) - (2)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DOUBLE);
    }
    break;

  case 145:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIMESTAMP);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIMESTAMP, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 146:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIMESTAMP); }
    break;

  case 147:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 148:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 149:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 150:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 151:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CREATETIME); }
    break;

  case 152:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_MODIFYTIME); }
    break;

  case 153:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DATE);
      yyerror(&(yylsp[(1) - (1)]), result, "DATE type is not supported");
      YYABORT;
    }
    break;

  case 154:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIME);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIME, 1, (yyvsp[(2) - (2)].node));
      yyerror(&(yylsp[(1) - (2)]), result, "TIME type is not supported");
      YYABORT;
    }
    break;

  case 155:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(2) - (5)].node), (yyvsp[(4) - (5)].node)); }
    break;

  case 156:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 157:

    { (yyval.node) = NULL; }
    break;

  case 158:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 159:

    { (yyval.node) = NULL; }
    break;

  case 160:

    { (yyval.node) = NULL; }
    break;

  case 161:

    { (yyval.node) = NULL; }
    break;

  case 162:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 163:

    { (yyval.node) = NULL; }
    break;

  case 164:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 165:

    { (yyval.node) = NULL; }
    break;

  case 166:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node)); }
    break;

  case 167:

    { (yyval.node) = NULL; }
    break;

  case 168:

    {
      (void)((yyvsp[(2) - (2)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NOT_NULL);
    }
    break;

  case 169:

    {
      (void)((yyvsp[(1) - (1)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NULL);
    }
    break;

  case 170:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_DEFAULT, 1, (yyvsp[(2) - (2)].node)); }
    break;

  case 171:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_AUTO_INCREMENT); }
    break;

  case 172:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_PRIMARY_KEY); }
    break;

  case 173:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 174:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 175:

    {
      (yyval.node) = NULL;
    }
    break;

  case 176:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_INFO, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 177:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPIRE_INFO, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 178:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLET_MAX_SIZE, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 179:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLET_BLOCK_SIZE, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 180:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLET_ID, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 181:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_REPLICA_NUM, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 182:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COMPRESS_METHOD, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 183:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_USE_BLOOM_FILTER, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 184:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSISTENT_MODE);
      (yyval.node)->value_ = 1;
    }
    break;

  case 185:

    {
      (void)((yyvsp[(2) - (3)].node)); /*  make bison mute*/
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COMMENT, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 186:

    { (yyval.node) = NULL; }
    break;

  case 187:

    { (yyval.node) = NULL; }
    break;

  case 188:

    {
      ParseNode *tables = NULL;
      merge_nodes(tables, result->malloc_pool_, T_TABLE_LIST, (yyvsp[(4) - (4)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_DROP_TABLE, 2, (yyvsp[(3) - (4)].node), tables);
    }
    break;

  case 189:

    { (yyval.node) = NULL; }
    break;

  case 190:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_IF_EXISTS); }
    break;

  case 191:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 192:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 193:

    {
      ParseNode* val_list = NULL;
      merge_nodes(val_list, result->malloc_pool_, T_VALUE_LIST, (yyvsp[(6) - (7)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_INSERT, 6,
                              (yyvsp[(3) - (7)].node),           /* target relation */
                              (yyvsp[(4) - (7)].node),           /* column list */
                              val_list,     /* value list */
                              NULL,         /* value from sub-query */
                              (yyvsp[(1) - (7)].node),           /* is replacement */
                              (yyvsp[(7) - (7)].node)            /* when expression */
                              );
    }
    break;

  case 194:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_INSERT, 6,
                              (yyvsp[(3) - (4)].node),           /* target relation */
                              NULL,         /* column list */
                              NULL,         /* value list */
                              (yyvsp[(4) - (4)].node),           /* value from sub-query */
                              (yyvsp[(1) - (4)].node),           /* is replacement */
                              NULL          /* when expression */
                              );
    }
    break;

  case 195:

    {
      /* if opt_when is really needed, use select_with_parens instead */
      ParseNode* col_list = NULL;
      merge_nodes(col_list, result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(5) - (7)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_INSERT, 6,
                              (yyvsp[(3) - (7)].node),           /* target relation */
                              col_list,     /* column list */
                              NULL,         /* value list */
                              (yyvsp[(7) - (7)].node),           /* value from sub-query */
                              (yyvsp[(1) - (7)].node),           /* is replacement */
                              NULL          /* when expression */
                              );
    }
    break;

  case 196:

    { (yyval.node) = NULL; }
    break;

  case 197:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    }
    break;

  case 198:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    }
    break;

  case 199:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    }
    break;

  case 200:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(2) - (3)].node));
    }
    break;

  case 201:

    { (yyval.node) = NULL; }
    break;

  case 202:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 203:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 204:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_VALUE_VECTOR, (yyvsp[(2) - (3)].node));
    }
    break;

  case 205:

    {
    merge_nodes((yyvsp[(4) - (5)].node), result->malloc_pool_, T_VALUE_VECTOR, (yyvsp[(4) - (5)].node));
    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (5)].node), (yyvsp[(4) - (5)].node));
  }
    break;

  case 206:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 207:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 208:

    {
      (yyval.node) = (yyvsp[(1) - (2)].node);
      (yyval.node)->children_[14] = (yyvsp[(2) - (2)].node);
      if ((yyval.node)->children_[12] == NULL && (yyvsp[(2) - (2)].node) != NULL)
      {
        malloc_terminal_node((yyval.node)->children_[12], result->malloc_pool_, T_BOOL);
        (yyval.node)->children_[12]->value_ = 1;
      }
    }
    break;

  case 209:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 210:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 211:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 212:

    {
      (yyval.node)= (yyvsp[(1) - (1)].node);
    }
    break;

  case 213:

    {
      (yyval.node) = (yyvsp[(1) - (2)].node);
      (yyval.node)->children_[12] = (yyvsp[(2) - (2)].node);
    }
    break;

  case 214:

    {
      /* use the new order by to replace old one */
      ParseNode* select = (ParseNode*)(yyvsp[(1) - (3)].node);
      if (select->children_[10])
        destroy_tree(select->children_[10]);
      select->children_[10] = (yyvsp[(2) - (3)].node);
      select->children_[12] = (yyvsp[(3) - (3)].node);
      (yyval.node) = select;
    }
    break;

  case 215:

    {
      /* use the new order by to replace old one */
      ParseNode* select = (ParseNode*)(yyvsp[(1) - (4)].node);
      if ((yyvsp[(2) - (4)].node))
      {
        if (select->children_[10])
          destroy_tree(select->children_[10]);
        select->children_[10] = (yyvsp[(2) - (4)].node);
      }

      /* set limit value */
      if (select->children_[11])
        destroy_tree(select->children_[11]);
      select->children_[11] = (yyvsp[(3) - (4)].node);
      select->children_[12] = (yyvsp[(4) - (4)].node);
      (yyval.node) = select;
    }
    break;

  case 216:

    {
      ParseNode* project_list = NULL;
      merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, (yyvsp[(4) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 15,
                              (yyvsp[(3) - (5)].node),             /* 1. distinct */
                              project_list,   /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              (yyvsp[(5) - (5)].node),             /* 12. limit */
                              NULL,           /* 13. for update */
                              (yyvsp[(2) - (5)].node),             /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    }
    break;

  case 217:

    {
      ParseNode* project_list = NULL;
      merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, (yyvsp[(4) - (8)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 15,
                              (yyvsp[(3) - (8)].node),             /* 1. distinct */
                              project_list,   /* 2. select clause */
                              NULL,           /* 3. from clause */
                              (yyvsp[(7) - (8)].node),             /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              (yyvsp[(8) - (8)].node),             /* 12. limit */
                              NULL,           /* 13. for update */
                              (yyvsp[(2) - (8)].node),             /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    }
    break;

  case 218:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 219:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 220:

    {
      ParseNode* project_list = NULL;
      ParseNode* from_list = NULL;
      merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, (yyvsp[(4) - (9)].node));
      merge_nodes(from_list, result->malloc_pool_, T_FROM_LIST, (yyvsp[(6) - (9)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 15,
                              (yyvsp[(3) - (9)].node),             /* 1. distinct */
                              project_list,   /* 2. select clause */
                              from_list,      /* 3. from clause */
                              (yyvsp[(7) - (9)].node),             /* 4. where */
                              (yyvsp[(8) - (9)].node),             /* 5. group by */
                              (yyvsp[(9) - (9)].node),             /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              (yyvsp[(2) - (9)].node),             /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    }
    break;

  case 221:

    {
      ParseNode* set_op = NULL;
      malloc_terminal_node(set_op, result->malloc_pool_, T_SET_UNION);
	    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 15,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              (yyvsp[(3) - (4)].node),             /* 8. all specified? */
                              (yyvsp[(1) - (4)].node),             /* 9. former select stmt */
                              (yyvsp[(4) - (4)].node),             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              NULL,           /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    }
    break;

  case 222:

    {
      ParseNode* set_op = NULL;
      malloc_terminal_node(set_op, result->malloc_pool_, T_SET_INTERSECT);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 15,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              (yyvsp[(3) - (4)].node),             /* 8. all specified? */
                              (yyvsp[(1) - (4)].node),             /* 9. former select stmt */
                              (yyvsp[(4) - (4)].node),             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              NULL,           /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    }
    break;

  case 223:

    {
      ParseNode* set_op = NULL;
      malloc_terminal_node(set_op, result->malloc_pool_, T_SET_EXCEPT);
	    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 15,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              (yyvsp[(3) - (4)].node),             /* 8. all specified? */
                              (yyvsp[(1) - (4)].node),             /* 9. former select stmt */
                              (yyvsp[(4) - (4)].node),             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              NULL,           /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    }
    break;

  case 224:

    {(yyval.node) = NULL;}
    break;

  case 225:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    }
    break;

  case 226:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_WHERE_CLAUSE, 2, (yyvsp[(3) - (3)].node), (yyvsp[(2) - (3)].node));
    }
    break;

  case 227:

    {
      if ((yyvsp[(2) - (4)].node)->type_ == T_QUESTIONMARK && (yyvsp[(4) - (4)].node)->type_ == T_QUESTIONMARK)
      {
        (yyvsp[(4) - (4)].node)->value_++;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 228:

    {
      if ((yyvsp[(2) - (4)].node)->type_ == T_QUESTIONMARK && (yyvsp[(4) - (4)].node)->type_ == T_QUESTIONMARK)
      {
        (yyvsp[(4) - (4)].node)->value_++;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(4) - (4)].node), (yyvsp[(2) - (4)].node));
    }
    break;

  case 229:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(2) - (2)].node), NULL);
    }
    break;

  case 230:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, NULL, (yyvsp[(2) - (2)].node));
    }
    break;

  case 231:

    {
      if ((yyvsp[(2) - (4)].node)->type_ == T_QUESTIONMARK && (yyvsp[(4) - (4)].node)->type_ == T_QUESTIONMARK)
      {
        (yyvsp[(4) - (4)].node)->value_++;
      }
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(4) - (4)].node), (yyvsp[(2) - (4)].node));
    }
    break;

  case 232:

    {
      (yyval.node) = NULL;
    }
    break;

  case 233:

    {
      if ((yyvsp[(2) - (3)].node))
      {
        merge_nodes((yyval.node), result->malloc_pool_, T_HINT_OPTION_LIST, (yyvsp[(2) - (3)].node));
      }
      else
      {
        (yyval.node) = NULL;
      }
    }
    break;

  case 234:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 235:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 236:

    {
      (yyval.node) = NULL;
    }
    break;

  case 237:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 238:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    }
    break;

  case 239:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_READ_STATIC);
    }
    break;

  case 240:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_HOTSPOT);
    }
    break;

  case 241:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_READ_CONSISTENCY);
      (yyval.node)->value_ = (yyvsp[(3) - (4)].ival);
    }
    break;

  case 242:

    {
      (yyval.node) = (yyvsp[(2) - (3)].node);
    }
    break;

  case 243:

    {
      (yyval.node) = (yyvsp[(1) - (2)].node);
    }
    break;

  case 244:

    {
      (yyval.node) = NULL;
    }
    break;

  case 245:

    {
    (yyval.ival) = 3;
  }
    break;

  case 246:

    {
    (yyval.ival) = 4;
  }
    break;

  case 247:

    {
    (yyval.ival) = 1;
  }
    break;

  case 248:

    {
    (yyval.ival) = 2;
  }
    break;

  case 249:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 250:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 251:

    { (yyval.node) = NULL; }
    break;

  case 252:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 253:

    { (yyval.node) = NULL; }
    break;

  case 254:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    }
    break;

  case 255:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 0;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 256:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 0;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 257:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 1;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 258:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 2;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 259:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 0;
      ParseNode *default_operand = NULL;
      malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
      default_operand->str_value_ = " "; /* blank for default */
      default_operand->value_ = strlen(default_operand->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (3)].node));
    }
    break;

  case 260:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 1;
      ParseNode *default_operand = NULL;
      malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
      default_operand->str_value_ = " "; /* blank for default */
      default_operand->value_ = strlen(default_operand->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (3)].node));
    }
    break;

  case 261:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 2;
      ParseNode *default_operand = NULL;
      malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
      default_operand->str_value_ = " "; /* blank for default */
      default_operand->value_ = strlen(default_operand->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (3)].node));
    }
    break;

  case 262:

    { (yyval.node) = NULL; }
    break;

  case 263:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_EXPR_LIST, (yyvsp[(3) - (3)].node));
    }
    break;

  case 264:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 265:

    { (yyval.node) = NULL; }
    break;

  case 266:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_SORT_LIST, (yyvsp[(3) - (3)].node));
    }
    break;

  case 267:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 268:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 269:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SORT_KEY, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    }
    break;

  case 270:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SORT_ASC); }
    break;

  case 271:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SORT_ASC); }
    break;

  case 272:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SORT_DESC); }
    break;

  case 273:

    { (yyval.node) = 0; }
    break;

  case 274:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    }
    break;

  case 275:

    {
      (yyval.node) = NULL;
    }
    break;

  case 276:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_ALL);
    }
    break;

  case 277:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_DISTINCT);
    }
    break;

  case 278:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, (yyvsp[(1) - (1)].node));
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (1)]).first_column, (yylsp[(1) - (1)]).last_column);
    }
    break;

  case 279:

    {
      ParseNode* alias_node = NULL;
      malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, alias_node);
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (2)]).first_column, (yylsp[(1) - (2)]).last_column);
      dup_expr_string(alias_node->str_value_, result, (yylsp[(2) - (2)]).first_column, (yylsp[(2) - (2)]).last_column);
    }
    break;

  case 280:

    {
      ParseNode* alias_node = NULL;
      malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, alias_node);
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (3)]).first_column, (yylsp[(1) - (3)]).last_column);
      dup_expr_string(alias_node->str_value_, result, (yylsp[(3) - (3)]).first_column, (yylsp[(3) - (3)]).last_column);
    }
    break;

  case 281:

    {
      ParseNode* star_node = NULL;
      malloc_terminal_node(star_node, result->malloc_pool_, T_STAR);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, star_node);
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (1)]).first_column, (yylsp[(1) - (1)]).last_column);
    }
    break;

  case 282:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 283:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 284:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 285:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 286:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 287:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 288:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    }
    break;

  case 289:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 290:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    }
    break;

  case 291:

    {
    	(yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 292:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(2) - (5)].node), (yyvsp[(5) - (5)].node));
    	yyerror(&(yylsp[(1) - (5)]), result, "qualied joined table can not be aliased!");
      YYABORT;
    }
    break;

  case 293:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 294:

    {
    	(yyval.node) = (yyvsp[(2) - (3)].node);
    }
    break;

  case 295:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_JOINED_TABLE, 4, (yyvsp[(2) - (6)].node), (yyvsp[(1) - (6)].node), (yyvsp[(4) - (6)].node), (yyvsp[(6) - (6)].node));
    }
    break;

  case 296:

    {
      ParseNode* node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_JOIN_INNER);
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_JOINED_TABLE, 4, node, (yyvsp[(1) - (5)].node), (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    }
    break;

  case 297:

    {
      /* make bison mute */
      (void)((yyvsp[(2) - (2)].node));
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_FULL);
    }
    break;

  case 298:

    {
      /* make bison mute */
      (void)((yyvsp[(2) - (2)].node));
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_LEFT);
    }
    break;

  case 299:

    {
      /* make bison mute */
      (void)((yyvsp[(2) - (2)].node));
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_RIGHT);
    }
    break;

  case 300:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_INNER);
    }
    break;

  case 301:

    { (yyval.node) = NULL; }
    break;

  case 302:

    { (yyval.node) = NULL; }
    break;

  case 303:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPLAIN, 1, (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = ((yyvsp[(2) - (3)].node) ? 1 : 0); /* positive: verbose */
    }
    break;

  case 304:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 305:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 306:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 307:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 308:

    { (yyval.node) = (ParseNode*)1; }
    break;

  case 309:

    { (yyval.node) = NULL; }
    break;

  case 310:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_TABLES, 1, (yyvsp[(3) - (3)].node)); }
    break;

  case 311:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node)); }
    break;

  case 312:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node)); }
    break;

  case 313:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_TABLE_STATUS, 1, (yyvsp[(4) - (4)].node)); }
    break;

  case 314:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_SERVER_STATUS, 1, (yyvsp[(4) - (4)].node)); }
    break;

  case 315:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_VARIABLES, 1, (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = (yyvsp[(2) - (4)].ival);
    }
    break;

  case 316:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_SCHEMA); }
    break;

  case 317:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_CREATE_TABLE, 1, (yyvsp[(4) - (4)].node)); }
    break;

  case 318:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 319:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 320:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_WARNINGS, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 321:

    {
      if ((yyvsp[(2) - (3)].node)->type_ != T_FUN_COUNT)
      {
        yyerror(&(yylsp[(1) - (3)]), result, "Only COUNT(*) function is supported in SHOW WARNINGS statement!");
        YYABORT;
      }
      else
      {
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_WARNINGS);
        (yyval.node)->value_ = 1;
      }
    }
    break;

  case 322:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_GRANTS, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 323:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_PARAMETERS, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 324:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_PROCESSLIST);
      (yyval.node)->value_ = (yyvsp[(2) - (3)].ival);
    }
    break;

  case 325:

    {
      if ((yyvsp[(2) - (4)].node)->value_ < 0 || (yyvsp[(4) - (4)].node)->value_ < 0)
      {
        yyerror(&(yylsp[(1) - (4)]), result, "OFFSET/COUNT must not be less than 0!");
        YYABORT;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_LIMIT, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 326:

    {
      if ((yyvsp[(2) - (2)].node)->value_ < 0)
      {
        yyerror(&(yylsp[(1) - (2)]), result, "COUNT must not be less than 0!");
        YYABORT;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_LIMIT, 2, NULL, (yyvsp[(2) - (2)].node));
    }
    break;

  case 327:

    { (yyval.node) = NULL; }
    break;

  case 328:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 329:

    { (yyval.node) = NULL; }
    break;

  case 330:

    { (yyval.node) = NULL; }
    break;

  case 331:

    { (yyval.ival) = 1; }
    break;

  case 332:

    { (yyval.ival) = 0; }
    break;

  case 333:

    { (yyval.ival) = 0; }
    break;

  case 334:

    { (yyval.node) = NULL; }
    break;

  case 335:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LIKE, 1, (yyvsp[(2) - (2)].node)); }
    break;

  case 336:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_WHERE_CLAUSE, 1, (yyvsp[(2) - (2)].node)); }
    break;

  case 337:

    { (yyval.node) = NULL; }
    break;

  case 338:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LIKE, 1, (yyvsp[(1) - (1)].node)); }
    break;

  case 339:

    { (yyval.ival) = 0; }
    break;

  case 340:

    { (yyval.ival) = 1; }
    break;

  case 341:

    {
        merge_nodes((yyval.node), result->malloc_pool_, T_CREATE_USER, (yyvsp[(3) - (3)].node));
    }
    break;

  case 342:

    {
        (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 343:

    {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 344:

    {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CREATE_USER_SPEC, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 345:

    {
        (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 346:

    {
        (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 347:

    {
        merge_nodes((yyval.node), result->malloc_pool_, T_DROP_USER, (yyvsp[(3) - (3)].node));
    }
    break;

  case 348:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 349:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 350:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SET_PASSWORD, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    }
    break;

  case 351:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SET_PASSWORD, 2, (yyvsp[(3) - (6)].node), (yyvsp[(6) - (6)].node));
    }
    break;

  case 352:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    }
    break;

  case 353:

    {
      (yyval.node) = NULL;
    }
    break;

  case 354:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_RENAME_USER, (yyvsp[(3) - (3)].node));
    }
    break;

  case 355:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_RENAME_INFO, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 356:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 357:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 358:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LOCK_USER, 2, (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 359:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    }
    break;

  case 360:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    }
    break;

  case 361:

    {
      (void)(yyval.node);
    }
    break;

  case 362:

    {
      (void)(yyval.node);
    }
    break;

  case 363:

    {
      (yyval.ival) = 1;
    }
    break;

  case 364:

    {
      (yyval.ival) = 0;
    }
    break;

  case 365:

    {
      (void)(yyvsp[(2) - (2)].node);
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BEGIN);
      (yyval.node)->value_ = 0;
    }
    break;

  case 366:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BEGIN);
      (yyval.node)->value_ = (yyvsp[(3) - (3)].ival);
    }
    break;

  case 367:

    {
      (void)(yyvsp[(2) - (2)].node);
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_COMMIT);
    }
    break;

  case 368:

    {
      (void)(yyvsp[(2) - (2)].node);
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_ROLLBACK);
    }
    break;

  case 369:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_KILL, 3, (yyvsp[(2) - (4)].node), (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 370:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    }
    break;

  case 371:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    }
    break;

  case 372:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    }
    break;

  case 373:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    }
    break;

  case 374:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    }
    break;

  case 375:

    {
      ParseNode *privileges_node = NULL;
      ParseNode *users_node = NULL;
      merge_nodes(privileges_node, result->malloc_pool_, T_PRIVILEGES, (yyvsp[(2) - (6)].node));
      merge_nodes(users_node, result->malloc_pool_, T_USERS, (yyvsp[(6) - (6)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_GRANT,
                                 3, privileges_node, (yyvsp[(4) - (6)].node), users_node);
    }
    break;

  case 376:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 377:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 378:

    {
      (void)(yyvsp[(2) - (2)].node);                 /* useless */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_ALL;
    }
    break;

  case 379:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_ALTER;
    }
    break;

  case 380:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_CREATE;
    }
    break;

  case 381:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_CREATE_USER;
    }
    break;

  case 382:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_DELETE;
    }
    break;

  case 383:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_DROP;
    }
    break;

  case 384:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_GRANT_OPTION;
    }
    break;

  case 385:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_INSERT;
    }
    break;

  case 386:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_UPDATE;
    }
    break;

  case 387:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_SELECT;
    }
    break;

  case 388:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_REPLACE;
    }
    break;

  case 389:

    {
      (void)(yyval.node);
    }
    break;

  case 390:

    {
      (void)(yyval.node);
    }
    break;

  case 391:

    {
      /* means global priv_level */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_LEVEL);
    }
    break;

  case 392:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_LEVEL, 1, (yyvsp[(1) - (1)].node));
    }
    break;

  case 393:

    {
      ParseNode *privileges_node = NULL;
      ParseNode *priv_level = NULL;
      merge_nodes(privileges_node, result->malloc_pool_, T_PRIVILEGES, (yyvsp[(2) - (5)].node));
      if ((yyvsp[(3) - (5)].node) == NULL)
      {
        /* check privileges: should have and only have ALL PRIVILEGES, GRANT OPTION */
        int check_ok = 0;
        if (2 == privileges_node->num_child_)
        {
          assert(privileges_node->children_[0]->num_child_ == 0);
          assert(privileges_node->children_[0]->type_ == T_PRIV_TYPE);
          assert(privileges_node->children_[1]->num_child_ == 0);
          assert(privileges_node->children_[1]->type_ == T_PRIV_TYPE);
          if ((privileges_node->children_[0]->value_ == OB_PRIV_ALL
               && privileges_node->children_[1]->value_ == OB_PRIV_GRANT_OPTION)
              || (privileges_node->children_[1]->value_ == OB_PRIV_ALL
                  && privileges_node->children_[0]->value_ == OB_PRIV_GRANT_OPTION))
          {
            check_ok = 1;
          }
        }
        if (!check_ok)
        {
          yyerror(&(yylsp[(1) - (5)]), result, "support only ALL PRIVILEGES, GRANT OPTION");
          YYABORT;
        }
      }
      else
      {
        priv_level = (yyvsp[(3) - (5)].node);
      }
      ParseNode *users_node = NULL;
      merge_nodes(users_node, result->malloc_pool_, T_USERS, (yyvsp[(5) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_REVOKE,
                                 3, privileges_node, priv_level, users_node);
    }
    break;

  case 394:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    }
    break;

  case 395:

    {
      (yyval.node) = NULL;
    }
    break;

  case 396:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PREPARE, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 397:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 398:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 399:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 400:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 401:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 402:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_VARIABLE_SET, (yyvsp[(2) - (2)].node));;
      (yyval.node)->value_ = 2;
    }
    break;

  case 403:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 404:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 405:

    {
      (void)((yyvsp[(2) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = 2;
    }
    break;

  case 406:

    {
      (void)((yyvsp[(2) - (3)].node));
      (yyvsp[(1) - (3)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = 2;
    }
    break;

  case 407:

    {
      (void)((yyvsp[(3) - (4)].node));
      (yyvsp[(2) - (4)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = 1;
    }
    break;

  case 408:

    {
      (void)((yyvsp[(3) - (4)].node));
      (yyvsp[(2) - (4)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = 2;
    }
    break;

  case 409:

    {
      (void)((yyvsp[(4) - (5)].node));
      (yyvsp[(3) - (5)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
      (yyval.node)->value_ = 1;
    }
    break;

  case 410:

    {
      (void)((yyvsp[(4) - (5)].node));
      (yyvsp[(3) - (5)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
      (yyval.node)->value_ = 2;
    }
    break;

  case 411:

    {
      (void)((yyvsp[(2) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = 2;
    }
    break;

  case 412:

    { (yyval.node) = NULL; }
    break;

  case 413:

    { (yyval.node) = NULL; }
    break;

  case 414:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 415:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXECUTE, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 416:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_ARGUMENT_LIST, (yyvsp[(2) - (2)].node));
    }
    break;

  case 417:

    {
      (yyval.node) = NULL;
    }
    break;

  case 418:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 419:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 420:

    {
      (void)((yyvsp[(1) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_DEALLOCATE, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 421:

    { (yyval.node) = NULL; }
    break;

  case 422:

    { (yyval.node) = NULL; }
    break;

  case 423:

    {
      ParseNode *alter_actions = NULL;
      merge_nodes(alter_actions, result->malloc_pool_, T_ALTER_ACTION_LIST, (yyvsp[(4) - (4)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_TABLE, 2, (yyvsp[(3) - (4)].node), alter_actions);
    }
    break;

  case 424:

    {
      yyerror(&(yylsp[(1) - (6)]), result, "Table rename is not supported now");
      YYABORT;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLE_RENAME, 1, (yyvsp[(6) - (6)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_ACTION_LIST, 1, (yyval.node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_TABLE, 2, (yyvsp[(3) - (6)].node), (yyval.node));
    }
    break;

  case 425:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 426:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 427:

    {
      (void)((yyvsp[(2) - (3)].node)); /* make bison mute */
      (yyval.node) = (yyvsp[(3) - (3)].node); /* T_COLUMN_DEFINITION */
    }
    break;

  case 428:

    {
      (void)((yyvsp[(2) - (4)].node)); /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_DROP, 1, (yyvsp[(3) - (4)].node));
      (yyval.node)->value_ = (yyvsp[(4) - (4)].ival);
    }
    break;

  case 429:

    {
      (void)((yyvsp[(2) - (4)].node)); /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_ALTER, 2, (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 430:

    {
      (void)((yyvsp[(2) - (5)].node)); /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_RENAME, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    }
    break;

  case 431:

    { (yyval.node) = NULL; }
    break;

  case 432:

    { (yyval.node) = NULL; }
    break;

  case 433:

    { (yyval.ival) = 2; }
    break;

  case 434:

    { (yyval.ival) = 1; }
    break;

  case 435:

    { (yyval.ival) = 0; }
    break;

  case 436:

    {
      (void)((yyvsp[(3) - (3)].node)); /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NOT_NULL);
    }
    break;

  case 437:

    {
      (void)((yyvsp[(3) - (3)].node)); /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NULL);
    }
    break;

  case 438:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_DEFAULT, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 439:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_NULL);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_DEFAULT, 1, (yyval.node));
    }
    break;

  case 440:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_SYTEM_ACTION_LIST, (yyvsp[(4) - (4)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_SYSTEM, 1, (yyval.node));
    }
    break;

  case 441:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_MASTER_SLAVE);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CHANGE_OBI, 3, node, (yyvsp[(7) - (7)].node), (yyvsp[(3) - (7)].node));
    }
    break;

  case 442:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_MASTER_SLAVE);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CHANGE_OBI, 3, node, (yyvsp[(7) - (7)].node), (yyvsp[(3) - (7)].node));
    }
    break;

  case 443:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_MASTER);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CHANGE_OBI, 2, node, (yyvsp[(6) - (6)].node));
    }
    break;

  case 444:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_SLAVE);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CHANGE_OBI, 2, node, (yyvsp[(6) - (6)].node));
    }
    break;

  case 445:

    {
      (yyval.node) = NULL;
    }
    break;

  case 446:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_FORCE);
    }
    break;

  case 447:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 448:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 449:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SYSTEM_ACTION, 5,
                               (yyvsp[(1) - (9)].node),    /* param_name */
                               (yyvsp[(3) - (9)].node),    /* param_value */
                               (yyvsp[(4) - (9)].node),    /* comment */
                               (yyvsp[(8) - (9)].node),    /* server type */
                               (yyvsp[(9) - (9)].node)     /* cluster or IP/port */
                               );
      (yyval.node)->value_ = (yyvsp[(5) - (9)].ival);                /* scope */
    }
    break;

  case 450:

    { (yyval.node) = (yyvsp[(2) - (2)].node); }
    break;

  case 451:

    { (yyval.node) = NULL; }
    break;

  case 452:

    { (yyval.ival) = 0; }
    break;

  case 453:

    { (yyval.ival) = 1; }
    break;

  case 454:

    { (yyval.ival) = 2; }
    break;

  case 455:

    { (yyval.ival) = 2; }
    break;

  case 456:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 1;
    }
    break;

  case 457:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 4;
    }
    break;

  case 458:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 2;
    }
    break;

  case 459:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 3;
    }
    break;

  case 460:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CLUSTER, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 461:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SERVER_ADDRESS, 2, (yyvsp[(3) - (6)].node), (yyvsp[(6) - (6)].node));
    }
    break;

  case 462:

    { (yyval.node) = NULL; }
    break;

  case 463:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 464:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_IDENT);
      (yyval.node)->str_value_ = parse_strdup((yyvsp[(1) - (1)].non_reserved_keyword)->keyword_name, result->malloc_pool_);
      if ((yyval.node)->str_value_ == NULL)
      {
        yyerror(NULL, result, "No more space for string duplicate");
        YYABORT;
      }
      else
      {
        (yyval.node)->value_ = strlen((yyval.node)->str_value_);
      }
    }
    break;

  case 465:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 466:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_IDENT);
      (yyval.node)->str_value_ = parse_strdup((yyvsp[(1) - (1)].non_reserved_keyword)->keyword_name, result->malloc_pool_);
      if ((yyval.node)->str_value_ == NULL)
      {
        yyerror(NULL, result, "No more space for string duplicate");
        YYABORT;
      }
      else
      {
        (yyval.node)->value_ = strlen((yyval.node)->str_value_);
      }
    }
    break;

  case 468:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 469:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_IDENT);
      (yyval.node)->str_value_ = parse_strdup((yyvsp[(1) - (1)].non_reserved_keyword)->keyword_name, result->malloc_pool_);
      if ((yyval.node)->str_value_ == NULL)
      {
        yyerror(NULL, result, "No more space for string duplicate");
        YYABORT;
      }
    }
    break;



      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;
  *++yylsp = yyloc;

  /* Now `shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */

  yyn = yyr1[yyn];

  yystate = yypgoto[yyn - YYNTOKENS] + *yyssp;
  if (0 <= yystate && yystate <= YYLAST && yycheck[yystate] == *yyssp)
    yystate = yytable[yystate];
  else
    yystate = yydefgoto[yyn - YYNTOKENS];

  goto yynewstate;


/*------------------------------------.
| yyerrlab -- here on detecting error |
`------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYEMPTY : YYTRANSLATE (yychar);

  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (&yylloc, result, YY_("syntax error"));
#else
# define YYSYNTAX_ERROR yysyntax_error (&yymsg_alloc, &yymsg, \
                                        yyssp, yytoken)
      {
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = YYSYNTAX_ERROR;
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == 1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = (char *) YYSTACK_ALLOC (yymsg_alloc);
            if (!yymsg)
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = 2;
              }
            else
              {
                yysyntax_error_status = YYSYNTAX_ERROR;
                yymsgp = yymsg;
              }
          }
        yyerror (&yylloc, result, yymsgp);
        if (yysyntax_error_status == 2)
          goto yyexhaustedlab;
      }
# undef YYSYNTAX_ERROR
#endif
    }

  yyerror_range[1] = yylloc;

  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
	 error, discard it.  */

      if (yychar <= YYEOF)
	{
	  /* Return failure if at end of input.  */
	  if (yychar == YYEOF)
	    YYABORT;
	}
      else
	{
	  yydestruct ("Error: discarding",
		      yytoken, &yylval, &yylloc, result);
	  yychar = YYEMPTY;
	}
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:

  /* Pacify compilers like GCC when the user code never invokes
     YYERROR and the label yyerrorlab therefore never appears in user
     code.  */
  if (/*CONSTCOND*/ 0)
     goto yyerrorlab;

  yyerror_range[1] = yylsp[1-yylen];
  /* Do not reclaim the symbols of the rule which action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;	/* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
	{
	  yyn += YYTERROR;
	  if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
	    {
	      yyn = yytable[yyn];
	      if (0 < yyn)
		break;
	    }
	}

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
	YYABORT;

      yyerror_range[1] = *yylsp;
      yydestruct ("Error: popping",
		  yystos[yystate], yyvsp, yylsp, result);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  *++yyvsp = yylval;

  yyerror_range[2] = yylloc;
  /* Using YYLLOC is tempting, but would change the location of
     the lookahead.  YYLOC is available though.  */
  YYLLOC_DEFAULT (yyloc, yyerror_range, 2);
  *++yylsp = yyloc;

  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;

/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;

#if !defined(yyoverflow) || YYERROR_VERBOSE
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (&yylloc, result, YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif

yyreturn:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, &yylloc, result);
    }
  /* Do not reclaim the symbols of the rule which action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
		  yystos[*yyssp], yyvsp, yylsp, result);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  /* Make sure YYID is used.  */
  return YYID (yyresult);
}





void yyerror(YYLTYPE* yylloc, ParseResult* p, char* s, ...)
{
  if (p != NULL)
  {
    p->result_tree_ = 0;
    va_list ap;
    va_start(ap, s);
    vsnprintf(p->error_msg_, MAX_ERROR_MSG, s, ap);
    if (yylloc != NULL)
    {
      if (p->input_sql_[yylloc->first_column - 1] != '\'')
        p->start_col_ = yylloc->first_column;
      p->end_col_ = yylloc->last_column;
      p->line_ = yylloc->first_line;
    }
  }
}

int parse_init(ParseResult* p)
{
  int ret = 0;  // can not include C++ file "ob_define.h"
  if (!p || !p->malloc_pool_)
  {
    ret = -1;
    if (p)
    {
      snprintf(p->error_msg_, MAX_ERROR_MSG, "malloc_pool_ must be set");
    }
  }
  if (ret == 0)
  {
    ret = yylex_init_extra(p, &(p->yyscan_info_));
  }
  return ret;
}

int parse_terminate(ParseResult* p)
{
  return yylex_destroy(p->yyscan_info_);
}

int parse_sql(ParseResult* p, const char* buf, size_t len)
{
  int ret = -1;
  p->result_tree_ = 0;
  p->error_msg_[0] = 0;
  p->input_sql_ = buf;
  p->input_sql_len_ = len;
  p->start_col_ = 1;
  p->end_col_ = 1;
  p->line_ = 1;
  p->yycolumn_ = 1;
  p->yylineno_ = 1;
  p->tmp_literal_ = NULL;

  if (buf == NULL || len <= 0)
  {
    snprintf(p->error_msg_, MAX_ERROR_MSG, "Input SQL can not be empty");
    return ret;
  }

  while(len > 0 && isspace(buf[len - 1]))
    --len;

  if (len <= 0)
  {
    snprintf(p->error_msg_, MAX_ERROR_MSG, "Input SQL can not be while space only");
    return ret;
  }

  YY_BUFFER_STATE bp;

  //bp = yy_scan_string(buf, p->yyscan_info_);
  bp = yy_scan_bytes(buf, len, p->yyscan_info_);
  yy_switch_to_buffer(bp, p->yyscan_info_);
  ret = yyparse(p);
  yy_delete_buffer(bp, p->yyscan_info_);
  return ret;
}

