import globalValues
  
def printGlobal():  
    print(globalValues.GLOBAL_1)  
    print(globalValues.GLOBAL_3)  
    globalValues.GLOBAL_2 += 1 # modify values  
  
if __name__ == '__main__':  
    for i in range(0,100):
        printGlobal()
        print(globalValues.GLOBAL_2) 
