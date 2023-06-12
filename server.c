#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <signal.h>
#include <time.h>

#define SERVER_PORT 8080
#define MAX_PENDING_REQUESTS 100
#define MAX_MESSAGES 100
#define MAX_ORDERS 100
#define BUFFER_SIZE 1024

int serverSocket;
int serverSeqNum = 1;

typedef struct {
    int clientId;
    int lastSeqNum;
    char compId[10];
    FILE* logFile;
} ClientInfo;

typedef struct NewOrderSingle {
    char clOrdId[20];
    int clientId;
    char instrument[20];
    char side[5];
    int quantity;
    double price;
    struct NewOrderSingle* next;
} NewOrderSingle;

typedef struct {
    char instrument[20];
    double lastPx;
} MarketData;

MarketData marketDataList[MAX_ORDERS];
int marketDataCount = 0;

ClientInfo clientList[MAX_PENDING_REQUESTS];
NewOrderSingle* buyOrders = NULL;
NewOrderSingle* sellOrders = NULL;
int clientCount = 0;

char sentMessages[MAX_MESSAGES][BUFFER_SIZE];
int sentMessagesCount = 0;

void handleInterrupt(int signum) {
    for (int i = 0; i < clientCount; i++) {
        fclose(clientList[i].logFile);
    }
    close(serverSocket);
    exit(EXIT_SUCCESS);
}

int generateCheckSum(const char* message) {
    int checksum = 0;
    int len = strlen(message);
    for (int i = 0; i < len; i++) {
        checksum += message[i];
    }
    return checksum;
}

void generateSendingTime(char* timeStr) {
    time_t now = time(NULL);
    struct tm* tm_info = localtime(&now);
    strftime(timeStr, 21, "%Y%m%d-%H:%M:%S", tm_info);
}

double findLastPx(const char* instrument) {
    for (int i = 0; i < marketDataCount; i++) {
        if (strcmp(marketDataList[i].instrument, instrument) == 0) {
            return marketDataList[i].lastPx;
        }
    }
    return -1.0;  // Special value to indicate that the instrument was not found
}

void writeLog(FILE* logFile, const char* message) {
    char timeStr[21];
    generateSendingTime(timeStr);
    fprintf(logFile, "[%s] %s\n", timeStr, message);
    fflush(logFile);
}

void parseNewOrderSingle(const char* message, NewOrderSingle* order) {
    sscanf(message, "CompID=%*[^|]|ServerSeqNum=%*d|ClientSeqNum=%*d|MsgType=%*[^|]|ClOrdID=%[^|]|Instrument=%[^|]|Side=%[^|]|Quantity=%d|Price=%lf",
           order->clOrdId, order->instrument, order->side, &order->quantity, &order->price);
}

void formatNewOrderSingle(const NewOrderSingle* order, char* message) {
    sprintf(message, "CompID=SERVER|ServerSeqNum=0|ClientSeqNum=0|MsgType=NewOrderSingle|ClOrdID=%s|Instrument=%s|Side=%s|Quantity=%d|Price=%.2f|",
            order->clOrdId, order->instrument, order->side, order->quantity, order->price);
    int checksum = generateCheckSum(message);
    sprintf(message + strlen(message), "SendingTime=YYYYMMDD-HH:MM:SS|CheckSum=%d|", checksum);
}

void handleNewOrderSingle(ClientInfo* client, NewOrderSingle* order, FILE* logFile, NewOrderSingle** buyOrders, NewOrderSingle** sellOrders, int* serverSeqNum, MarketData* marketDataList, int* marketDataCount) {
    char orderDetails[BUFFER_SIZE];
    sprintf(orderDetails, "ClientID: %d, ClOrdID: %s, Instrument: %s, Side: %s, Quantity: %d, Price: %.2f",
            client->clientId, order->clOrdId, order->instrument, order->side, order->quantity, order->price);
    writeLog(logFile, orderDetails);
    fflush(logFile);

    printf("%s\n", orderDetails);

    NewOrderSingle** ownOrderList;
    NewOrderSingle** oppositeOrderList;
    int isBuyOrder = strcmp(order->side, "BUY") == 0;
    if (isBuyOrder) {
        ownOrderList = buyOrders;
        oppositeOrderList = sellOrders;
    } else {
        ownOrderList = sellOrders;
        oppositeOrderList = buyOrders;
    }

    NewOrderSingle* prevOrder = NULL;
    NewOrderSingle* currentOrder = *oppositeOrderList;
    while (currentOrder != NULL) {
        if (strcmp(currentOrder->instrument, order->instrument) == 0 && currentOrder->quantity == order->quantity) {
            if ((isBuyOrder && currentOrder->price <= order->price) ||
                (!isBuyOrder && currentOrder->price >= order->price)) {
                printf("Match found: %s\n", orderDetails);

                client->lastSeqNum++;
                (*serverSeqNum)++;

                if (prevOrder == NULL) {
                    *oppositeOrderList = currentOrder->next;
                } else {
                    prevOrder->next = currentOrder->next;
                }
                free(currentOrder);

                // Send message to client about completed order
                printf("Match found: %s\n", orderDetails);

                return;
            }
        }
        prevOrder = currentOrder;
        currentOrder = currentOrder->next;
    }

    NewOrderSingle* newOrder = (NewOrderSingle*) malloc(sizeof(NewOrderSingle));
    if (newOrder == NULL) {
        // Handle error, e.g., by logging and returning
        fprintf(logFile, "Failed to allocate memory for new order.\n");
        fflush(logFile);
        return;
    }
    *newOrder = *order;
    newOrder->clientId = client->clientId;  // Remember to set the client ID in the new order
    newOrder->next = *ownOrderList;
    *ownOrderList = newOrder;

    client->lastSeqNum++;
    (*serverSeqNum)++;

    // Only add sell orders to market data list
    if (!isBuyOrder) {
        strcpy(marketDataList[*marketDataCount].instrument, order->instrument);
        marketDataList[*marketDataCount].lastPx = order->price;
        (*marketDataCount)++;
    }
}

void handleMarketDataRequest(ClientInfo* client, const char* message, int clientSocket, char* buffer) {
    char instrument[20];
    sscanf(message, "CompID=%*[^|]|ServerSeqNum=%*d|ClientSeqNum=%*d|MsgType=%*[^|]|Instrument=%[^|]|",
           instrument);

    writeLog(client->logFile, "Market data request received.");
    fflush(client->logFile);

    double lastPx = findLastPx(instrument);
    if (lastPx >= 0) {
        sprintf(buffer, "CompID=SERVER|ServerSeqNum=%d|ClientSeqNum=%d|MsgType=W|Instrument=%s|LastPx=%.2f|",
                serverSeqNum++, client->lastSeqNum++, instrument, lastPx);
    } else {
        sprintf(buffer, "CompID=SERVER|ServerSeqNum=%d|ClientSeqNum=%d|MsgType=3|Text=Instrument not found|",
                serverSeqNum++, client->lastSeqNum++);
    }
    send(clientSocket, buffer, strlen(buffer), 0);
}


void handleLogon(ClientInfo* client, int clientSocket, char* buffer) {
    char compId[10];
    sscanf(buffer, "CompID=%[^|]|", compId);

    client->clientId = clientSocket;
    client->lastSeqNum = 0;
    strncpy(client->compId, compId, sizeof(client->compId));

    char logFileName[20];
    sprintf(logFileName, "%s.log", client->compId);

    char fullPath[1024]; 
    const char *directoryPath = "/Users/alpaltug/Desktop/code/staj'23/cboe/";
    sprintf(fullPath, "%s/%s", directoryPath, logFileName);

    client->logFile = fopen(fullPath, "w");
    if (clientList[clientCount].logFile == NULL) {
        perror("Error in opening log file");
        exit(EXIT_FAILURE);
    }

    writeLog(client->logFile, "Client successfully logged on.");

    strcpy(buffer, "Logon successful.");
    ssize_t bytesSent = send(clientSocket, buffer, strlen(buffer), 0);
    if (bytesSent < 0) {
        perror("Error in sending data");
        exit(EXIT_FAILURE);
    }
}

void handleTestRequest(ClientInfo* client, int clientSocket, char* buffer) {
    writeLog(client->logFile, "Client test request received.");
    fflush(client->logFile);

    strcpy(buffer, "TestResponse");
    ssize_t bytesSent = send(clientSocket, buffer, strlen(buffer), 0);
    if (bytesSent < 0) {
        perror("Error in sending data");
        exit(EXIT_FAILURE);
    }
}

void handleResendRequest(ClientInfo* client, const char* message, int clientSocket) {
    int beginSeqNo, endSeqNo;
    sscanf(message, "CompID=%*[^|]|ServerSeqNum=%*d|ClientSeqNum=%*d|MsgType=%*[^|]|BeginSeqNo=%d|EndSeqNo=%d|",
           &beginSeqNo, &endSeqNo);

    writeLog(client->logFile, "Resend request received.");
    fflush(client->logFile);

    // Resend requested messages
    for (int i = beginSeqNo - 1; i < endSeqNo && i < sentMessagesCount; i++) {
        ssize_t bytesSent = send(clientSocket, sentMessages[i], strlen(sentMessages[i]), 0);
        if (bytesSent < 0) {
            perror("Error in sending data");
            exit(EXIT_FAILURE);
        }
    }
}

void handleOrderCancelRequest(ClientInfo* client, const char* message, int clientSocket, char* buffer, NewOrderSingle** buyOrders, NewOrderSingle** sellOrders, MarketData* marketDataList, int* marketDataCount) {
    char clOrdId[20];
    sscanf(message, "CompID=%*[^|]|ServerSeqNum=%*d|ClientSeqNum=%*d|MsgType=%*[^|]|ClOrdID=%[^|]|",
           clOrdId);

    writeLog(client->logFile, "Order cancel request received.");
    fflush(client->logFile);

    // Cancel order
    NewOrderSingle* prevOrder = NULL;
    NewOrderSingle* currentOrder = buyOrders;
    while (currentOrder != NULL) {
        if (strcmp(currentOrder->clOrdId, clOrdId) == 0) {
            if (prevOrder == NULL) {
                *buyOrders = currentOrder->next;
            } else {
                prevOrder->next = currentOrder->next;
            }
            free(currentOrder);
            break;
        }
        prevOrder = currentOrder;
        currentOrder = currentOrder->next;
    }

    prevOrder = NULL;
    currentOrder = *sellOrders;
    while (currentOrder != NULL) {
        if (strcmp(currentOrder->clOrdId, clOrdId) == 0) {
            if (prevOrder == NULL) {
                *sellOrders = currentOrder->next;
            } else {
                prevOrder->next = currentOrder->next;
            }
            free(currentOrder);
            break;
        }
        prevOrder = currentOrder;
        currentOrder = currentOrder->next;
    }

    // Remove from market data
    for (int i = 0; i < *marketDataCount; i++) {
        if (strcmp(marketDataList[i].instrument, clOrdId) == 0) {
            // Shift all elements down
            for (int j = i; j < *marketDataCount - 1; j++) {
                marketDataList[j] = marketDataList[j + 1];
            }
            (*marketDataCount)--;
            break;
        }
    }

    // Send response to client
    sprintf(buffer, "Order with ClOrdID=%s has been cancelled.", clOrdId);
    ssize_t bytesSent = send(clientSocket, buffer, strlen(buffer), 0);
    if (bytesSent < 0) {
        perror("Error in sending data");
        exit(EXIT_FAILURE);
    }
}
void handleClientMessage(ClientInfo* client, const char* message, int clientSocket, char* buffer) {
    char msgType[20];
    sscanf(message, "CompID=%*[^|]|ServerSeqNum=%*d|ClientSeqNum=%*d|MsgType=%[^|]|",
           msgType);

    if (strcmp(msgType, "NewOrderSingle") == 0) {
        NewOrderSingle order;
        parseNewOrderSingle(message, &order);
        order.clientId = client->clientId;  
        handleNewOrderSingle(client, &order, client->logFile, &buyOrders, &sellOrders, &serverSeqNum);

        sprintf(buffer, "Received order: %s,%s,%s,%d,%.2f",
            order.clOrdId, order.instrument, order.side, order.quantity, order.price);
        ssize_t bytesSent = send(clientSocket, buffer, strlen(buffer), 0);
        if (bytesSent < 0) {
            perror("Error in sending data");
            exit(EXIT_FAILURE);
        }
    } else if (strcmp(msgType, "Logon") == 0) {
        handleLogon(client, clientSocket, buffer);
    } else if (strcmp(msgType, "TestRequest") == 0) {
        handleTestRequest(client, clientSocket, buffer);
    } else if (strcmp(msgType, "ResendRequest") == 0) {
        handleResendRequest(client, message, clientSocket);
    } else if (strcmp(msgType, "OrderCancelRequest") == 0) {
        handleOrderCancelRequest(client, message, clientSocket, buffer);
    } else if (msgType == 'V') {
        handleMarketDataRequest(&client, buffer, clientSocket, buffer);
    } else {
        writeLog(client->logFile, "Invalid message type");
        fflush(client->logFile);
    }
}

void handleClient(int clientId) {
    char buffer[BUFFER_SIZE];
    ClientInfo* client = NULL;
    while (1) {
        memset(buffer, 0, sizeof(buffer));
        read(clientId, buffer, sizeof(buffer));

        if (client == NULL) {
            for (int i = 0; i < clientCount; i++) {
                if (clientList[i].clientId == clientId) {
                    client = &clientList[i];
                    break;
                }
            }
            if (client == NULL) {
                if (clientCount >= MAX_PENDING_REQUESTS) {
                    printf("Maximum number of clients exceeded.\n");
                    return;
                }
                client = &clientList[clientCount++];
            }
        }

        handleClientMessage(client, buffer, clientId, buffer);
    }
}

int main(int argc, char *argv[]) {
    struct sockaddr_in serverAddress, clientAddress;
    socklen_t clientAddressLength = sizeof(clientAddress);
    int clientSocket;

    serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket < 0) {
        perror("Cannot open socket");
        return EXIT_FAILURE;
    }

    memset(&serverAddress, 0, sizeof(serverAddress));
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    serverAddress.sin_port = htons(SERVER_PORT);

    if (bind(serverSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) < 0) {
        perror("Cannot bind socket");
        return EXIT_FAILURE;
    }

    listen(serverSocket, MAX_PENDING_REQUESTS);

    signal(SIGINT, handleInterrupt);

    while (1) {
        clientSocket = accept(serverSocket, (struct sockaddr*)&clientAddress, &clientAddressLength);
        if (clientSocket < 0) {
            perror("Cannot accept client");
            return EXIT_FAILURE;
        }

        if (fork() == 0) {  // Child process
            close(serverSocket);
            handleClient(clientSocket);
            exit(EXIT_SUCCESS);
        } else {  // Parent process
            close(clientSocket);
        }
    }

    return EXIT_SUCCESS;
}
