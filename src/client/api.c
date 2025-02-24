#include "api.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h> 
#include <errno.h>

// Variáveis globais para os caminhos dos pipes
static char req_pipe_path[MAX_PIPE_PATH_LENGTH];
static char resp_pipe_path[MAX_PIPE_PATH_LENGTH];
static char notif_pipe_path[MAX_PIPE_PATH_LENGTH];

// Guarda os pipe paths
void store_pipe_paths(const char *req_path, const char *resp_path, const char *notif_path) {
    strncpy(req_pipe_path, req_path, MAX_PIPE_PATH_LENGTH);
    strncpy(resp_pipe_path, resp_path, MAX_PIPE_PATH_LENGTH);
    strncpy(notif_pipe_path, notif_path, MAX_PIPE_PATH_LENGTH);
}

int kvs_connect(char const *req_path, char const *resp_path,
                char const *server_pipe_path, char const *notif_path,
                int *notif_pipe) {

    // Remover named pipes existentes, se houver
    unlink(req_path);
    unlink(resp_path);
    unlink(notif_path);

    // Criar named pipes
    if (mkfifo(req_path, 0666) == -1 && errno != EEXIST) {
        perror("mkfifo req_pipe");
        return 1;
    }
    if (mkfifo(resp_path, 0666) == -1 && errno != EEXIST) {
        perror("mkfifo resp_pipe");
        return 1;
    }
    if (mkfifo(notif_path, 0666) == -1 && errno != EEXIST) {
        perror("mkfifo notif_pipe");
        return 1;
    }

    // Store pipe paths
    store_pipe_paths(req_path, resp_path, notif_path);
   

    // Abrir pipe de notificação
    *notif_pipe = open(notif_path, O_RDONLY | O_NONBLOCK);
    if (*notif_pipe == -1) {
        perror("open notif_pipe");
        return 1;
    }

    // Enviar pedido de conexão ao servidor
    int server_fd = open(server_pipe_path, O_WRONLY);
    if (server_fd == -1) {
        perror("open server_pipe");
        return 1;
    }

    // Enviar os caminhos dos named pipes como três strings separadas
    char request_message[3 * MAX_PIPE_PATH_LENGTH + 1];
    request_message[0] = OP_CODE_CONNECT;
    strncpy(request_message + 1, req_pipe_path, MAX_PIPE_PATH_LENGTH);
    strncpy(request_message + 1 + MAX_PIPE_PATH_LENGTH, resp_pipe_path, MAX_PIPE_PATH_LENGTH);
    strncpy(request_message + 1 + 2 * MAX_PIPE_PATH_LENGTH, notif_pipe_path, MAX_PIPE_PATH_LENGTH);

    if (write(server_fd, request_message, sizeof(request_message)) == -1) {
        perror("write server_pipe");
        close(server_fd);
        return 1;
    }


    // Ler resposta do servidor
    char response[2];
    int resp_fd = open(resp_path, O_RDONLY);
    if (resp_fd == -1) {
        perror("open resp_pipe");
        close(server_fd);
        return 1;
    }

    if (read(resp_fd, response, sizeof(response)) == -1) {
        perror("read resp_pipe");
        close(resp_fd);
        close(server_fd);
        return 1;
    }

    printf("Server returned %d for operation: connect\n", response[1]);

    close(resp_fd);
    close(server_fd);
    return response[1];
}


int kvs_disconnect(void) {

    // Enviar pedido de desconexão ao servidor
    int req_fd = open(req_pipe_path, O_WRONLY);
    if (req_fd == -1) {
        perror("open req_pipe");
        return 1;
    }

    char msg[1];
    msg[0] = OP_CODE_DISCONNECT;

    if (write(req_fd, msg, sizeof(msg)) == -1) {
        perror("write req_pipe");
        close(req_fd);
        return 1;
    }

    close(req_fd);

    // Ler resposta do servidor
    int resp_fd = open(resp_pipe_path, O_RDONLY);
    if (resp_fd == -1) {
        perror("open resp_pipe");
        return 1;
    }

    char response[2];
    if (read(resp_fd, response, sizeof(response)) == -1) {
        perror("read resp_pipe");
        close(resp_fd);
        return 1;
    }

    printf("Server returned %d for operation: disconnect\n", response[1]);

    close(resp_fd);

    // Remover named pipes
    unlink(req_pipe_path);
    unlink(resp_pipe_path);
    unlink(notif_pipe_path);

    return response[1];
}

int kvs_subscribe(const char *key) {
    int req_fd = open(req_pipe_path, O_WRONLY);
    if (req_fd == -1) {
        perror("open req_pipe");
        return 1;
    }

    char msg[42];
    snprintf(msg, sizeof(msg), "%c%-40s", OP_CODE_SUBSCRIBE, key); // Preencher com espaços se necessário
    if (write(req_fd, msg, sizeof(msg)) == -1) {
        perror("write req_pipe");
        close(req_fd);
        return 1;
    }

    close(req_fd);

    int resp_fd = open(resp_pipe_path, O_RDONLY);
    if (resp_fd == -1) {
        perror("open resp_pipe");
        return 1;
    }

    char response[2];
    if (read(resp_fd, response, sizeof(response)) == -1) {
        perror("read resp_pipe");
        close(resp_fd);
        return 1;
    }


    close(resp_fd);
    printf("Server returned %d for operation: subscribe\n", response[1]);
    return response[1];
}

int kvs_unsubscribe(const char *key) {
    int req_fd = open(req_pipe_path, O_WRONLY);
    if (req_fd == -1) {
        perror("open req_pipe");
        return 1;
    }

    char msg[42];
    snprintf(msg, sizeof(msg), "%c%-40s", OP_CODE_UNSUBSCRIBE, key); // Preencher com espaços se necessário
    if (write(req_fd, msg, sizeof(msg)) == -1) {
        perror("write req_pipe");
        close(req_fd);
        return 1;
    }

    close(req_fd);

    int resp_fd = open(resp_pipe_path, O_RDONLY);
    if (resp_fd == -1) {
        perror("open resp_pipe");
        return 1;
    }

    char response[2];
    if (read(resp_fd, response, sizeof(response)) == -1) {
        perror("read resp_pipe");
        close(resp_fd);
        return 1;
    }

    close(resp_fd);
    printf("Server returned %d for operation: unsubscribe\n", response[1]);
    return response[1];
}   

