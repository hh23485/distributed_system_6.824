> result.txt
for i in {1..1000}
do
  echo ${i}
  go test -run 2A -race  > testp.log
  if [[ $? -ne 0 ]]; then
    break
  fi
done >> result.txt