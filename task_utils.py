import os

def createOutputDirectoryFromFilename(fileName):
    dirName = os.path.dirname(fileName)
    if not os.path.exists(dirName):
        os.makedirs(dirName)