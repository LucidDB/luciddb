grep '\[select .*\]' $1 | sed -e 's/^.*\[select \(.*\)\].*/select \1\;/g'> log2sql.out
