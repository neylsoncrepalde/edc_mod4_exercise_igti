set -x

for j in $(kubectl get sparkapplication -n airflow -o custom-columns=:.metadata.name)
do
    kubectl delete sparkapplication $j -n airflow &
done