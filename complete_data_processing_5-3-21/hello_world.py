import pyspark
sc = pyspark.SparkContext('local[*]')

txt = sc.textFile('file:////home/symptots/Desktop/big_data/complete_data_processing(5-3-21)/hello.txt')
print(txt.count())

python_lines = txt.filter(lambda line: 'python' in line.lower())
print(python_lines.count())