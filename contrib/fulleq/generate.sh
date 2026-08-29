#!/bin/bash -e

type="$1"
template="$2"

if [ "$type" = "packaged" ]; then
	echo '\echo Use "CREATE EXTENSION fulleq" to load this file. \quit'
elif [ "$type" = "unpackaged" ]; then
	echo '\echo Use "CREATE EXTENSION fulleq FROM unpackaged" to load this file. \quit'
	echo 'DROP OPERATOR CLASS IF EXISTS int2vector_fill_ops USING hash;'
	echo 'DROP OPERATOR FAMILY IF EXISTS int2vector_fill_ops USING hash;'
	echo 'DROP FUNCTION IF EXISTS fullhash_int2vector(int2vector);'
	echo 'DROP OPERATOR IF EXISTS == (int2vector, int2vector);'
	echo 'DROP FUNCTION IF EXISTS isfulleq_int2vector(int2vector, int2vector);'
else
	echo "invalid arguments"
	exit 1
fi


ARGTYPE=(
	bool
	bytea
	char
	name
	int8
	int2
	int4
	text
	oid
	xid
	cid
	oidvector
	float4
	float8
	macaddr
	inet
	cidr
	varchar
	date
	time
	timestamp
	timestamptz
	interval
	timetz
)

for type in	"${ARGTYPE[@]}"; do
	sed -e "s/ARGTYPE/$type/g" < "$template"
done
