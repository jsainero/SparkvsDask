output = open('gsod_20mb.csv', 'w')
file = open('gsod_s15.csv', 'r', encoding='utf8')
i = 0
for linea in file:
    output.write(linea)
    i = i+1
    if i == 191864:
        break
output.close()
file.close()
