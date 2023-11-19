import mmh3

def AssignUserToBucket(userId:str, numberlineSalt:str, numBuckets:int = 1000)->int:
    """This function randomly assings a userId to one of the buckets numbered 0 to numBuckets-1 
    on a specific numberline defined by numberlineSalt
    """
    if not userId:
        return -1
    if not numberlineSalt:
        return -1
    if numBuckets < 1:
        return -1
    
    hashString = str(userId) + str(numberlineSalt)
    hash = abs(mmh3.hash(hashString))
    bucket = hash % numBuckets
    return bucket


