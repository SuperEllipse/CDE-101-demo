curl -u pauldefusco https://service.cde-dtv7dn96.demo-aws.ylcu-atmi.cloudera.site/gateway/authtkn/knoxtoken/api/v1/token
curl -H "Authorization: Bearer ${CDE_TOKEN}" -X PUT "${CDE_JOB_URL}/resources/cde_REPORTS/KPI_Reports.py" -F 'file=@/home/cdsw/LC_KPI_Reporting.py'
curl -H "Authorization: Bearer ${CDE_TOKEN}" -X PUT "${CDE_JOB_URL}/resources/cde_REPORTS/KPI_Reports.py" -F 'file=@LC_KPI_Reporting.py'
echo $CDE_TOKEN
curl -H "Authorization: Bearer ${CDE_TOKEN}" -X PUT "${CDE_JOB_URL}/resources/cde_REPORTS/KPI_Reports.py" -F 'file=LC_KPI_Reporting.py'
curl -H "Authorization: Bearer ${CDE_TOKEN}" -X PUT "${CDE_JOB_URL}/resources/cde_REPORTS/KPI_Reports_2.py" -F 'file=LC_KPI_Reporting.py'
echo $CDE_JOB
curl -H "Authorization: Bearer ${CDE_TOKEN}" -X PUT "${CDE_JOB_URL}/resources/cde_REPORTS/KPI_Reports.py" -F 'file=@/LC_KPI_Reporting.py'
echo $CDE_JOB_URL
curl -H "Authorization: Bearer ${CDE_TOKEN}" -X GET "${CDE_JOB_URL}/resources/cde_REPORTS" | jq .
apt install jq
sudo apt install jq
