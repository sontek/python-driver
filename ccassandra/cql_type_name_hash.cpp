/* C++ code produced by gperf version 3.0.3 */
/* Command-line: /Library/Developer/CommandLineTools/usr/bin/gperf --language=C++ --readonly-tables --enum --class-name=CqlTypeNameHash --multiple-iterations=10 --struct-type --lookup-function-name=FromString cql_type_names.gperf  */
/* Computed positions: -k'1' */

#if !((' ' == 32) && ('!' == 33) && ('"' == 34) && ('#' == 35) \
      && ('%' == 37) && ('&' == 38) && ('\'' == 39) && ('(' == 40) \
      && (')' == 41) && ('*' == 42) && ('+' == 43) && (',' == 44) \
      && ('-' == 45) && ('.' == 46) && ('/' == 47) && ('0' == 48) \
      && ('1' == 49) && ('2' == 50) && ('3' == 51) && ('4' == 52) \
      && ('5' == 53) && ('6' == 54) && ('7' == 55) && ('8' == 56) \
      && ('9' == 57) && (':' == 58) && (';' == 59) && ('<' == 60) \
      && ('=' == 61) && ('>' == 62) && ('?' == 63) && ('A' == 65) \
      && ('B' == 66) && ('C' == 67) && ('D' == 68) && ('E' == 69) \
      && ('F' == 70) && ('G' == 71) && ('H' == 72) && ('I' == 73) \
      && ('J' == 74) && ('K' == 75) && ('L' == 76) && ('M' == 77) \
      && ('N' == 78) && ('O' == 79) && ('P' == 80) && ('Q' == 81) \
      && ('R' == 82) && ('S' == 83) && ('T' == 84) && ('U' == 85) \
      && ('V' == 86) && ('W' == 87) && ('X' == 88) && ('Y' == 89) \
      && ('Z' == 90) && ('[' == 91) && ('\\' == 92) && (']' == 93) \
      && ('^' == 94) && ('_' == 95) && ('a' == 97) && ('b' == 98) \
      && ('c' == 99) && ('d' == 100) && ('e' == 101) && ('f' == 102) \
      && ('g' == 103) && ('h' == 104) && ('i' == 105) && ('j' == 106) \
      && ('k' == 107) && ('l' == 108) && ('m' == 109) && ('n' == 110) \
      && ('o' == 111) && ('p' == 112) && ('q' == 113) && ('r' == 114) \
      && ('s' == 115) && ('t' == 116) && ('u' == 117) && ('v' == 118) \
      && ('w' == 119) && ('x' == 120) && ('y' == 121) && ('z' == 122) \
      && ('{' == 123) && ('|' == 124) && ('}' == 125) && ('~' == 126))
/* The character set is not based on ISO-646.  */
#error "gperf generated tables don't work with this execution character set. Please report a bug to <bug-gnu-gperf@gnu.org>."
#endif

#line 1 "cql_type_names.gperf"

#include <cstring>
#line 4 "cql_type_names.gperf"
struct CqlTypeNameIdentification { const char *name; CqlTypeName typeName; };
/* maximum key range = 54, duplicates = 0 */

class CqlTypeNameHash
{
private:
  static inline unsigned int hash (const char *str, unsigned int len);
public:
  static const struct CqlTypeNameIdentification *FromString (const char *str, unsigned int len);
};

inline unsigned int
CqlTypeNameHash::hash (register const char *str, register unsigned int len)
{
  static const unsigned char asso_values[] =
    {
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57,  0,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 18,  6, 15,
      12, 57, 11, 57, 57, 11, 57, 57, 17, 17,
      57, 57, 57, 57, 57,  0,  0,  7,  0, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57, 57, 57, 57, 57,
      57, 57, 57, 57, 57, 57
    };
  return len + asso_values[(unsigned char)str[0]];
}

const struct CqlTypeNameIdentification *
CqlTypeNameHash::FromString (register const char *str, register unsigned int len)
{
  enum
    {
      TOTAL_KEYWORDS = 26,
      MIN_WORD_LENGTH = 3,
      MAX_WORD_LENGTH = 56,
      MIN_HASH_VALUE = 3,
      MAX_HASH_VALUE = 56
    };

  static const struct CqlTypeNameIdentification wordlist[] =
    {
      {""}, {""}, {""},
#line 24 "cql_type_names.gperf"
      {"set", CqlSetTypeName},
#line 28 "cql_type_names.gperf"
      {"text", CqlTextTypeName},
#line 27 "cql_type_names.gperf"
      {"tuple", CqlTupleTypeName},
#line 19 "cql_type_names.gperf"
      {"varint", CqlIntegerTypeName},
#line 31 "cql_type_names.gperf"
      {"varchar", CqlVarcharTypeName},
#line 25 "cql_type_names.gperf"
      {"timeuuid", CqlTimeUuidTypeName},
#line 26 "cql_type_names.gperf"
      {"timestamp", CqlTimestampTypeName},
#line 8 "cql_type_names.gperf"
      {"blob", CqlBlobTypeName},
#line 29 "cql_type_names.gperf"
      {"uuid", CqlUuidTypeName},
#line 21 "cql_type_names.gperf"
      {"bigint", CqlLongTypeName},
#line 7 "cql_type_names.gperf"
      {"boolean", CqlBooleanTypeName},
#line 18 "cql_type_names.gperf"
      {"int", CqlInt32TypeName},
#line 17 "cql_type_names.gperf"
      {"inet", CqlInetTypeName},
#line 15 "cql_type_names.gperf"
      {"float", CqlFloatTypeName},
#line 16 "cql_type_names.gperf"
      {"frozen", CqlFrozenTypeName},
#line 13 "cql_type_names.gperf"
      {"double", CqlDoubleTypeName},
#line 12 "cql_type_names.gperf"
      {"decimal", CqlDecimalTypeName},
#line 22 "cql_type_names.gperf"
      {"map", CqlMapTypeName},
#line 20 "cql_type_names.gperf"
      {"list", CqlListTypeName},
#line 11 "cql_type_names.gperf"
      {"counter", CqlCounterTypeName},
#line 6 "cql_type_names.gperf"
      {"ascii", CqlAsciiTypeName},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 30 "cql_type_names.gperf"
      {"'org.apache.cassandra.db.marshal.UserType'", CqlUserTypeTypeName},
      {""}, {""}, {""},
#line 23 "cql_type_names.gperf"
      {"'org.apache.cassandra.db.marshal.ReversedType'", CqlReversedTypeTypeName},
#line 10 "cql_type_names.gperf"
      {"'org.apache.cassandra.db.marshal.CompositeType'", CqlCompositeTypeName},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 14 "cql_type_names.gperf"
      {"'org.apache.cassandra.db.marshal.DynamicCompositeType'", CqlDynamicCompositeTypeName},
      {""},
#line 9 "cql_type_names.gperf"
      {"'org.apache.cassandra.db.marshal.ColumnToCollectionType'", CqlColumnToCollectionTypeName}
    };

  if (len <= MAX_WORD_LENGTH && len >= MIN_WORD_LENGTH)
    {
      unsigned int key = hash (str, len);

      if (key <= MAX_HASH_VALUE)
        {
          register const char *s = wordlist[key].name;

          if (*str == *s && !strcmp (str + 1, s + 1))
            return &wordlist[key];
        }
    }
  return 0;
}
#line 32 "cql_type_names.gperf"

