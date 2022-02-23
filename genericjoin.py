def GenericJoin(relations, tables, index):
	if index == len(attribute)-1:
		return SetIntersect(Project(relations, tables, index))
	L = SetIntersect(Project(relations, tables, index))
	result = []
	for value in L:
		tablesatv = []
		for i in range(len(tables)):
			tablesatv.append(Select(relations[i], tables[i], index, value))
		Qv = GenericJoin(relations, tablesatv, index+1)
		for t in Qv:
			result.append(value + ',' + t)
	return result

def Select(relation, table, index, value):
	if attribute[index] not in relation:
		return(table)
	temp = []
	indexx = relation.index(attribute[index])
	for j in range(len(table)):
		if table[j][indexx] == value:
			if table[j] not in temp:
				temp.append(table[j])
	return temp


def SetIntersect(tables):
	result = []
	min = len(tables[0])
	index = 0
	for i in range(len(tables)):
		if min > len(tables[i]):
			min = len(tables[i])
			index = i
	for j in range(len(tables[index])):
		flag = 1
		for k in range(len(tables)):
			if tables[index][j] not in tables[k]:
				flag =  0
				break
		if flag == 1:
			result.append(tables[index][j])
	return result

def Project(relations, tables, index):
	result=[]
	I = attribute[index]
	for i in range(len(tables)):
		if I in relations[i]:
			L = []
			index = relations[i].index(I)
			for t in tables[i]:
				if t[index] not in L:
					L.append(t[index])
			result.append(L)	
	return result


relations = []
tables = []
f = open("/Users/xiao/Desktop/input.txt")
text = f.readlines()
i = -1
table = []
for line in text:
	if 'R' in line:
		if i >= 0:	
			tables.append(table)
		i+=1
		begin = line.index('(')
		end = line.index(')')
		relations.append(line[begin+1:end].split(","))
		table = []
		continue
	if line != '\n':
		table.append(line.replace('\n','').split(","))

tables.append(table)
print tables
attribute = []	
for i in range(len(relations)):
	for j in range(len(relations[i])):
		if relations[i][j] not in attribute:
			attribute.append(relations[i][j])

print attribute
Join = GenericJoin(relations, tables, 0)
print Join


