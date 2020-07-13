output = open('gsod_s15.csv', 'w')
file = open('gsod_s15.txt', 'r', encoding='utf8')
for linea in file:
    linea_partida = linea.split()
    linea_partida = [i.replace('*', '') for i in linea_partida]
    linea_csv = ','.join(linea_partida)
    output.write(linea_csv)
    output.write('\n')
output.close()
