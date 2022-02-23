import random
import time

N = 100000
time = 1000
domain = 100

def data_generate(n, domain, attributes, N):
	result = ""
	for i in range(N):
		left = random.randint(0,time)
		right = random.randint(0,time)
		if left > right:
			mid = left
			left = right
			right = mid
		if left == right:
			right += 1
		row = ""
		for j in range(n):
			row += str(random.randint(0,domain)) + ','
		row +=  str(left) + ',' + str(right) + '\n'
		result += row
	return result

R1 = data_generate(2,domain,['a','b'],N)
R2 = data_generate(3,domain,['a','b','d'],N)
R3 = data_generate(3,domain,['a','b','e'],N)
R4 = data_generate(3,domain,['a','c','f'],N)
R5 = data_generate(3,domain,['a','c','g'],N)

f = open("data/test-db-large/R1_AB.txt", "w")
f.write(R1)
f.close()

f = open("data/test-db-large/R2_ABD.txt", "w")
f.write(R2)
f.close()

f = open("data/test-db-large/R3_ABE.txt", "w")
f.write(R3)
f.close()

f = open("data/test-db-large/R4_ACF.txt", "w")
f.write(R4)
f.close()

f = open("data/test-db-large/R5_ACG.txt", "w")
f.write(R5)
f.close()