#pragma once // Ensure this is here
#include"semanticParser.h"

void executeCommand();

void executeCLEAR();
void executeCROSS();
void executeDISTINCT();
void executeEXPORT();
void executeINDEX();
void executeJOIN();
void executeLIST();
void executeLOAD();
void executePRINT();
void executePROJECTION();
void executeRENAME();
void executeSELECTION();
void executeSORT();
void executeSOURCE();

void executeLOAD_GRAPH();
void executeEXPORT_GRAPH();
void executePATH();
void executeDEGREE();
void executeSETBUFFER();
void executeGROUP_BY();
void executeTRANSACTION();

bool isFileExists(string tableName);
bool isQueryFile(string fileName);

bool evaluateBinOp(int value1, int value2, BinaryOperator binaryOperator);
void printRowCount(int rowCount);