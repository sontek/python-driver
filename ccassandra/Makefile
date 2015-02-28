all: cql_type_name_hash.cpp

cql_type_name_hash.cpp: cql_type_names.gperf
	gperf --language=C++ \
	      --readonly-tables \
	      --enum \
	      --class-name=CqlTypeNameHash \
	      --multiple-iterations=10 \
	      --struct-type \
	      --lookup-function-name=FromString \
	      cql_type_names.gperf > cql_type_name_hash.cpp

.PHONY: all
