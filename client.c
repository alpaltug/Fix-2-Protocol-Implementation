#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <time.h>

#define SERVER_ADDRESS "127.0.0.1"
#define SERVER_PORT 8080
#define BUFFER_SIZE 1024
#define CLIENT_ID 1

int clientSeqNum = 1;
char compId[10] = "CLIENT1";

typedef struct {
    char clOrdId[20];
    char symbol[10];
    char side[5];
    int orderQty;
    char ordType[2];
    double price;
} ExecutionReport;

typedef struct {
    char clOrdId[20];
    char origClOrdId[20];
    char ordStatus[2];
} OrderCancelReject;

typedef struct {
    char instrument[20];
    char side[5];
    int quantity;
    double price;
} NewOrderSingle;

typedef struct {
    char clOrdId[20];
} OrderCancelRequest;

void generateSendingTime(char* timeStr, size_t size) {
    time_t now = time(NULL);
    struct tm* tm_info = localtime(&now);
    strftime(timeStr, size, "%Y%m%d-%H:%M:%S", tm_info);
}

void writeLog(FILE* logFile, const char* message) {
    char timeStr[21];
    generateSendingTime(timeStr, sizeof(timeStr));
    fprintf(logFile, "[%s] %s\n", timeStr, message);
    if (fflush(logFile) == EOF) {
        perror("Error in flushing log file");
    }
}

void formatHeartbeatMessage(char* message) {
    char sendingTime[21];
    generateSendingTime(sendingTime, sizeof(sendingTime));
    snprintf(message, BUFFER_SIZE, "8=FIX.4.2|35=0|49=%s|56=SERVER|34=%d|52=%s|",
            compId, clientSeqNum++, sendingTime);
    int checksum = 0;
    for (char *p = message; *p; p++) {
        checksum += (unsigned char)*p;
    }
    checksum %= 256;
    snprintf(message + strlen(message), BUFFER_SIZE - strlen(message), "10=%03d|", checksum);
}

int parseNewOrderSingle(const char* message, NewOrderSingle* order) {
    return sscanf(message, "%19[^,],%4[^,],%d,%lf",
           order->instrument, order->side, &order->quantity, &order->price);
}

void formatNewOrderSingle(const NewOrderSingle* order, char* message) {
    char sendingTime[21];
    generateSendingTime(sendingTime, sizeof(sendingTime));
    snprintf(message, BUFFER_SIZE, "8=FIX.4.2|35=D|49=%s|56=SERVER|34=%d|52=%s|",
            compId, clientSeqNum++, sendingTime);
    snprintf(message + strlen(message), BUFFER_SIZE - strlen(message), "55=%s|54=%s|38=%d|44=%.2f|",
            order->instrument, order->side, order->quantity, order->price);
    int checksum = 0; 
    for (char *p = message; *p; p++) {
        checksum += (unsigned char)*p;
    }
    checksum %= 256;
    snprintf(message + strlen(message), BUFFER_SIZE - strlen(message), "10=%03d|", checksum);
}

void formatLogonMessage(char* message) {
    char sendingTime[21];
    generateSendingTime(sendingTime, sizeof(sendingTime));
    snprintf(message, BUFFER_SIZE, "8=FIX.4.2|35=A|49=%s|56=SERVER|34=%d|52=%s|",
            compId, clientSeqNum++, sendingTime);
    int checksum = 0;
    for (char *p = message; *p; p++) {
        checksum += (unsigned char)*p;
    }
    checksum %= 256;
    snprintf(message + strlen(message), BUFFER_SIZE - strlen(message), "10=%03d|", checksum);
}

void resendFIXMessages(int clientSocket, int seqNum, FILE* logFile) {
    char message[BUFFER_SIZE] = {0};

    // Here you would typically retrieve the messages from seqNum to the current sequence number
    // from some form of message storage, like a database or an array of previously sent messages.
    // Since this is a simplified example, we'll just send a fixed message.
    snprintf(message, sizeof(message), "Resending messages from %d to %d", seqNum, clientSeqNum);

    sendFIXMessage(clientSocket, message, logFile);
}

void setSequenceNumber(int newSeqNum) {
    clientSeqNum = newSeqNum;
}

void formatTestRequestMessage(char* message) {
    char sendingTime[21];
    generateSendingTime(sendingTime, sizeof(sendingTime));
    snprintf(message, BUFFER_SIZE, "8=FIX.4.2|35=1|49=%s|56=SERVER|34=%d|52=%s|",
            compId, clientSeqNum++, sendingTime);
    int checksum = 0;
    for (char *p = message; *p; p++) {
        checksum += (unsigned char)*p;
    }
    checksum %= 256;
    snprintf(message + strlen(message), BUFFER_SIZE - strlen(message), "10=%03d|", checksum);
}

int parseExecutionReport(const char* message, ExecutionReport* report) {
    return sscanf(message, "%*[^|]|35=8|%*[^|]|11=%19[^|]|%*[^|]|55=%9[^|]|%*[^|]|54=%4[^|]|%*[^|]|38=%d|%*[^|]|40=%1[^|]|%*[^|]|44=%lf",
           report->clOrdId, report->symbol, report->side, &report->orderQty, report->ordType, &report->price);
}

int parseOrderCancelReject(const char* message, OrderCancelReject* reject) {
    return sscanf(message, "%*[^|]|35=9|%*[^|]|11=%19[^|]|%*[^|]|41=%19[^|]|%*[^|]|39=%1[^|]",
           reject->clOrdId, reject->origClOrdId, reject->ordStatus);
}

void requestMarketData(int clientSocket, const char* instrument) {
    char buffer[BUFFER_SIZE];
    sprintf(buffer, "CompID=CLIENT|ServerSeqNum=0|ClientSeqNum=0|MsgType=V|Instrument=%s|", instrument);
    ssize_t bytesSent = send(clientSocket, buffer, strlen(buffer), 0);
    if (bytesSent < 0) {
        perror("Error in sending data");
        exit(EXIT_FAILURE);
    }
    // Receive server response
    memset(buffer, 0, sizeof(buffer));
    ssize_t bytesRead = recv(clientSocket, buffer, sizeof(buffer), 0);
    if (bytesRead < 0) {
        perror("Error in receiving data");
        exit(EXIT_FAILURE);
    }
    printf("Received: %s\n", buffer);
}


void handleIncomingMessage(const char* message, int clientSocket, FILE* logFile) {
    char msgType[3];
    int seqNum, newSeqNum;

    if (sscanf(message, "%*[^|]|35=%2[^|]", msgType) != 1) {
        printf("Invalid message format.\n");
        return;
    }

    if (strcmp(msgType, "0") == 0) {
        // This is a heartbeat message
        writeLog(logFile, "Received Heartbeat message");
    } else if (strcmp(msgType, "1") == 0) {
        // This is a test request message, send a Heartbeat message back
        char heartbeatMessage[BUFFER_SIZE] = {0};
        formatHeartbeatMessage(heartbeatMessage);
        sendFIXMessage(clientSocket, heartbeatMessage, logFile);
    } else if (strcmp(msgType, "2") == 0) {
        // This is a Resend Request, handle appropriately
        if (sscanf(message, "%*[^|]|34=%d[^|]", &seqNum) != 1) {
            printf("Invalid Resend Request format.\n");
            return;
        }
        // Resend messages from seqNum to current sequence number
        resendFIXMessages(clientSocket, seqNum, logFile);
        writeLog(logFile, "Handled Resend Request");
    } else if (strcmp(msgType, "4") == 0) {
        // This is a Sequence Reset, handle appropriately
        if (sscanf(message, "%*[^|]|123=%d[^|]", &newSeqNum) != 1) {
            printf("Invalid Sequence Reset format.\n");
            return;
        }
        // Update sequence number to newSeqNum
        setSequenceNumber(newSeqNum);
        writeLog(logFile, "Handled Sequence Reset");
    } else if (strcmp(msgType, "5") == 0) {
        // This is a logout message, close the connection
        printf("Received Logout message, closing connection...\n");
        close(clientSocket);
        if (fclose(logFile) != 0) {
            perror("Error in closing log file");
        }
        exit(EXIT_SUCCESS);
    } else if (strcmp(msgType, "8") == 0) {
        // This is an execution report message, parse it
        ExecutionReport report;
        if (parseExecutionReport(message, &report) == 6) {
            printf("Received Execution Report: ClOrdId=%s, Symbol=%s, Side=%s, OrderQty=%d, OrdType=%s, Price=%.2f\n",
                   report.clOrdId, report.symbol, report.side, report.orderQty, report.ordType, report.price);
        } else {
            printf("Invalid Execution Report format.\n");
        }
    } else if (strcmp(msgType, "9") == 0) {
        // This is an order cancel reject message, parse it
        OrderCancelReject reject;
        if (parseOrderCancelReject(message, &reject) == 3) {
            printf("Received Order Cancel Reject: ClOrdId=%s, OrigClOrdId=%s, Text=%s\n",
                   reject.clOrdId, reject.origClOrdId, reject.text);
        } else {
            printf("Invalid Order Cancel Reject format.\n");
        }
    } else {
        printf("Unknown message type: %s\n", msgType);
    }
}


void formatOrderCancelRequest(const OrderCancelRequest* request, char* message) {
    char sendingTime[21];
    generateSendingTime(sendingTime, sizeof(sendingTime));
    snprintf(message, BUFFER_SIZE, "8=FIX.4.2|35=F|49=%s|56=SERVER|34=%d|52=%s|",
            compId, clientSeqNum++, sendingTime);
    snprintf(message + strlen(message), BUFFER_SIZE - strlen(message) - 1, "41=%s|", request->clOrdId);
    int checksum = 0;
    for (char *p = message; *p; p++) {
        checksum += (unsigned char)*p;
    }
    checksum %= 256;
    snprintf(message + strlen(message), BUFFER_SIZE - strlen(message) - 1, "10=%03d|", checksum);
}

void sendFIXMessage(int clientSocket, const char* message, FILE* logFile) {
    ssize_t bytesSent = send(clientSocket, message, strlen(message), 0);
    if (bytesSent < 0) {
        perror("Error in sending data");
        exit(EXIT_FAILURE);
    }
    writeLog(logFile, message);
}

int main() {
    int clientSocket;
    struct sockaddr_in serverAddr;

    clientSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (clientSocket < 0) {
        perror("Error in socket creation");
        exit(EXIT_FAILURE);
    }

    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = inet_addr(SERVER_ADDRESS);
    serverAddr.sin_port = htons(SERVER_PORT);

    if (connect(clientSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        perror("Error in connecting to the server");
        exit(EXIT_FAILURE);
    }

    printf("Connected to the server.\n");

    char buffer[BUFFER_SIZE] = {0};
    ssize_t bytesRead = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
    if (bytesRead > 0) {
        printf("%s", buffer);
    } else if (bytesRead < 0) {
        perror("Error in receiving data");
        exit(EXIT_FAILURE);
    } else {
        printf("Server closed the connection.\n");
        close(clientSocket);
        exit(EXIT_SUCCESS);
    }

    FILE* logFile = fopen("//Users/alpaltug/Desktop/code/staj'23/cboe/client.log", "w");
    if (logFile == NULL) {
        perror("Error in creating log file");
        close(clientSocket);
        exit(EXIT_FAILURE);
    }

    // Send a logon message
    char logonMessage[BUFFER_SIZE] = {0};
    formatLogonMessage(logonMessage);
    sendFIXMessage(clientSocket, logonMessage, logFile);

    while (1) {
        printf("> ");
        if (fgets(buffer, BUFFER_SIZE, stdin) == NULL) {
            perror("Error in reading from stdin");
            break;
        }

        buffer[strcspn(buffer, "\n")] = '\0';

        if (strcmp(buffer, "testRequest") == 0) {
            // Send a test request message
            char testRequestMessage[BUFFER_SIZE] = {0};
            formatTestRequestMessage(testRequestMessage);
            sendFIXMessage(clientSocket, testRequestMessage, logFile);
        } else if (strcmp(buffer, "orderCancelRequest") == 0) {
            // Send an order cancel request message
            printf("Please enter ClOrdId of the order to cancel: ");
            char clOrdId[20];
            if (scanf("%19s", clOrdId) != 1) {
                printf("Invalid input.\n");
                continue;
            }
            getchar(); // consume newline

            OrderCancelRequest request;
            strncpy(request.clOrdId, clOrdId, sizeof(request.clOrdId) - 1);
            request.clOrdId[sizeof(request.clOrdId) - 1] = '\0';

            char orderCancelRequestMessage[BUFFER_SIZE] = {0};
            formatOrderCancelRequest(&request, orderCancelRequestMessage);
            sendFIXMessage(clientSocket, orderCancelRequestMessage, logFile);
        } else {
            NewOrderSingle order;
            if (parseNewOrderSingle(buffer, &order) != 4) {
                printf("Invalid command format. Please use: Instrument,Side,Quantity,Price\n");
                continue;
            }

            formatNewOrderSingle(&order, buffer);
            ssize_t bytesSent = send(clientSocket, buffer, strlen(buffer), 0);
            if (bytesSent < 0) {
                perror("Error in sending data");
                break;
            }
        }

        memset(buffer, 0, sizeof(buffer));
        bytesRead = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
        if (bytesRead > 0) {
            handleIncomingMessage(buffer, clientSocket, logFile);
        } else if (bytesRead < 0) {
            perror("Error in receiving data");
            break;
        } else {
            printf("Server closed the connection.\n");
            break;
        }
        writeLog(logFile, buffer);
    }

    close(clientSocket);
    if (fclose(logFile) != 0) {
        perror("Error in closing log file");
    }
    printf("Disconnected from the server.\n");

    return 0;
}
