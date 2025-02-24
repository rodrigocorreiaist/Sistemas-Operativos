#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/stat.h> // Include for mkfifo
#include <signal.h>   // Include for signal handling

#include "kvs.h"
#include "constants.h"
#include "io.h"
#include "operations.h"
#include "parser.h"
#include "src/common/protocol.h"
#include "src/common/constants.h"
#include "src/client/api.h"


struct SharedData {
  DIR *dir;
  char *dir_name;
  pthread_mutex_t directory_mutex;
};

struct ClientData {
  int resp_fd;
  int req_fd;
  int notif_fd;
  char req_pipe_path[40];
  char resp_pipe_path[40];
  char notif_pipe_path[40];
  int active;
  pthread_t thread;
  char subscribed_keys[MAX_NUMBER_SUB][MAX_STRING_SIZE];
  int num_subscribed_keys;
};

struct ClientData clients[MAX_NUMBER_SUB];
size_t num_clients = 0;
pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;
void *kvs_table = NULL; // Declare kvs_table

size_t active_backups = 0; // Number of active backups
size_t max_backups;        // Maximum allowed simultaneous backups
size_t max_threads;        // Maximum allowed simultaneous threads
char *jobs_directory = NULL;

volatile sig_atomic_t sigusr1_received = 0;


void unsubscribe_all_clients() {
  pthread_mutex_lock(&clients_mutex);

  for (size_t i = 0; i < num_clients; i++) {
    if (clients[i].active) {
      // Resetar as chaves subscritas e o número de subscrições
      memset(clients[i].subscribed_keys, 0, sizeof(clients[i].subscribed_keys));
      clients[i].num_subscribed_keys = 0;
    }
  }

  pthread_mutex_unlock(&clients_mutex);
}

void disconnect_all_clients() {
  pthread_mutex_lock(&clients_mutex);

  for (size_t i = 0; i < num_clients; i++) {
    if (clients[i].active) {
      // Fechar FIFOs de notificação e resposta
      if (clients[i].notif_fd != -1) {
        close(clients[i].notif_fd);
        clients[i].notif_fd = -1;
      }
      if (clients[i].resp_fd != -1) {
        close(clients[i].resp_fd);
        clients[i].resp_fd = -1;
      }
      if (clients[i].req_fd != -1) {
        close(clients[i].req_fd);
        clients[i].req_fd = -1;
      }

      // Dar unlink aos FIFOs
      unlink(clients[i].notif_pipe_path);
      unlink(clients[i].resp_pipe_path);
      unlink(clients[i].req_pipe_path);


      clients[i].active = 0;
    }
  }

  pthread_mutex_unlock(&clients_mutex);
}




void handle_sigusr1(int sig) {
  (void)sig; // Evitar aviso de parâmetro não utilizado
  sigusr1_received = 1; // Sinal recebido
  unsubscribe_all_clients();
  disconnect_all_clients();
}


int filter_job_files(const struct dirent *entry) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot != NULL && strcmp(dot, ".job") == 0) {
    return 1; // Keep this file (it has the .job extension)
  }
  return 0;
}

static int entry_files(const char *dir, struct dirent *entry, char *in_path,
                       char *out_path) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 ||
      strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

static int run_job(int in_fd, int out_fd, char *filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
    case CMD_WRITE:
      num_pairs =
          parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_write(num_pairs, keys, values)) {
        write_str(STDERR_FILENO, "Failed to write pair\n");
      }
      break;

    case CMD_READ:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_read(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to read pair\n");
      }
      break;

    case CMD_DELETE:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_delete(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to delete pair\n");
      }
      break;

    case CMD_SHOW:
      kvs_show(out_fd);
      break;

    case CMD_WAIT:
      if (parse_wait(in_fd, &delay, NULL) == -1) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay > 0) {
        printf("Waiting %d seconds\n", delay / 1000);
        kvs_wait(delay);
      }
      break;

    case CMD_BACKUP:
      pthread_mutex_lock(&n_current_backups_lock);
      if (active_backups >= max_backups) {
        wait(NULL);
      } else {
        active_backups++;
      }
      pthread_mutex_unlock(&n_current_backups_lock);
      int aux = kvs_backup(++file_backups, filename, jobs_directory);

      if (aux < 0) {
        write_str(STDERR_FILENO, "Failed to do backup\n");
      } else if (aux == 1) {
        return 1;
      }
      break;

    case CMD_INVALID:
      write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
      break;

    case CMD_HELP:
      write_str(STDOUT_FILENO,
                "Available commands:\n"
                "  WRITE [(key,value)(key2,value2),...]\n"
                "  READ [key,key2,...]\n"
                "  DELETE [key,key2,...]\n"
                "  SHOW\n"
                "  WAIT <delay_ms>\n"
                "  BACKUP\n" // Not implemented
                "  HELP\n");

      break;

    case CMD_EMPTY:
      break;

    case EOC:
      printf("EOF\n");
      return 0;
    }
  }
}

// frees arguments
static void *get_file(void *arguments) {
  struct SharedData *thread_data = (struct SharedData *)arguments;
  DIR *dir = thread_data->dir;
  char *dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent *entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

static void dispatch_threads(DIR *dir) {
  pthread_t *threads = malloc(max_threads * sizeof(pthread_t));

  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory,
                                   PTHREAD_MUTEX_INITIALIZER};

  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void *)&thread_data) !=
        0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  // ler do FIFO de registo

  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }

  free(threads);
}

int add_client(struct ClientData *new_client) {
    pthread_mutex_lock(&clients_mutex);


    if (num_clients >= MAX_NUMBER_SUB) {
        pthread_mutex_unlock(&clients_mutex);
        return -1; // Máximo de clientes alcançado
    }

    clients[num_clients] = *new_client;
    num_clients++;

    pthread_mutex_unlock(&clients_mutex);
    return 0; // Sucesso
}

int remove_client(const char *req_pipe_path) {
    pthread_mutex_lock(&clients_mutex);


    for (size_t i = 0; i < num_clients; i++) {
        if (strcmp(clients[i].req_pipe_path, req_pipe_path) == 0) {
            // Resetar as chaves subscritas e o número de subscrições
            memset(clients[i].subscribed_keys, 0, sizeof(clients[i].subscribed_keys));
            clients[i].num_subscribed_keys = 0;
            clients[i].active = 0; // Definir o campo active como 0
            pthread_mutex_unlock(&clients_mutex);
            return 0; // Subscrições do cliente removidas com sucesso
        }
    }

    pthread_mutex_unlock(&clients_mutex);
    return -1; // Cliente não encontrado
}


void *client_handler(void *arg) {
  struct ClientData *client_data = (struct ClientData *)arg;
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  pthread_sigmask(SIG_BLOCK, &set, NULL);

  int req_fd = open(client_data->req_pipe_path, O_RDONLY);
  if (req_fd == -1) {
    perror("open req_pipe");
    free(client_data);
    return NULL;
  }
  client_data->req_fd = req_fd;


  int notif_fd = open(client_data->notif_pipe_path, O_WRONLY | O_NONBLOCK);
  if (notif_fd == -1) {
    perror("open notif_pipe");
    close(req_fd);
    free(client_data);
    return NULL;
  }
  client_data->notif_fd = notif_fd;


  char buffer[128];
  while (1) {


    ssize_t bytes_read = read(req_fd, buffer, sizeof(buffer) - 1);

    if (bytes_read > 0) {
      buffer[bytes_read] = '\0';
      char op_code = buffer[0];
      int result = 1;
      switch (op_code) {
        case OP_CODE_SUBSCRIBE: {
          char key[MAX_STRING_SIZE];
          strncpy(key, buffer + 1, MAX_STRING_SIZE - 1);
          key[MAX_STRING_SIZE - 1] = '\0'; // Garantir que a chave é terminada com '\0'

          // Remover espaços em branco adicionais
          for (int i = strlen(key) - 1; i >= 0 && key[i] == ' '; i--) {
            key[i] = '\0';
          }

          // Check if key exists in the kvs table
          if (key_exists(kvs_table, key)) {
            result = 1;
            for (int i = 0; i < client_data->num_subscribed_keys; i++) {
              if (strcmp(client_data->subscribed_keys[i], key) == 0) {
                result = 0; // Key already subscribed
                break;
              }
            }

            if (result == 1 && client_data->num_subscribed_keys < MAX_NUMBER_SUB) {
              strncpy(client_data->subscribed_keys[client_data->num_subscribed_keys], key, MAX_STRING_SIZE);
              client_data->num_subscribed_keys++;
            }
          } else {
            result = 0; // Key does not exist in the kvs table
          }
          
          
          int resp_fd = open(client_data->resp_pipe_path, O_WRONLY);
          if (resp_fd != -1) {
            char response[2] = {OP_CODE_SUBSCRIBE, result};
            if (write(resp_fd, response, sizeof(response)) == -1) {
              perror("write resp_pipe");
            }
            close(resp_fd);
          } else {
            perror("open resp_pipe");

          }
          break;
        }


      case OP_CODE_UNSUBSCRIBE: {
          char key[MAX_STRING_SIZE];
          strncpy(key, buffer + 1, MAX_STRING_SIZE - 1);
          key[MAX_STRING_SIZE - 1] = '\0'; // Garantir que a chave é terminada com '\0'

          // Remover espaços em branco adicionais
          for (int i = strlen(key) - 1; i >= 0 && key[i] == ' '; i--) {
            key[i] = '\0';
          }

          // Remove subscription
          result = 1; // Inicialmente assume que a subscrição não existia
          for (int i = 0; i < client_data->num_subscribed_keys; i++) {
            if (strcmp(client_data->subscribed_keys[i], key) == 0) {
              result = 0; // Subscrição existia e foi removida
              for (int j = i; j < client_data->num_subscribed_keys - 1; j++) {
                strncpy(client_data->subscribed_keys[j], client_data->subscribed_keys[j + 1], MAX_STRING_SIZE);
              }
              client_data->num_subscribed_keys--;
              break;
            }
          }

          int resp_fd = open(client_data->resp_pipe_path, O_WRONLY);
          if (resp_fd != -1) {
            char response[2] = {OP_CODE_UNSUBSCRIBE, result};
            if (write(resp_fd, response, sizeof(response)) == -1) {
              perror("write resp_pipe");
            }
            close(resp_fd);
          } else {
            perror("open resp_pipe");
          }
          break;
        }

        case OP_CODE_DISCONNECT:
                    remove_client(client_data->req_pipe_path);

                    // Send response to client
                    int resp_fd = open(client_data->resp_pipe_path, O_WRONLY);
                    if (resp_fd != -1) {
                        char response[2] = {OP_CODE_DISCONNECT, 0}; // 0 indicates success
                        if (write(resp_fd, response, sizeof(response)) == -1) {
                            perror("write resp_pipe");
                        }
                        close(resp_fd);
                    } else {
                        perror("open resp_pipe");
                    }

                    close(req_fd);
                    close(notif_fd);
                    return NULL;

        default:
          fprintf(stderr, "Unknown operation code: %c (ASCII: %d)\n", op_code, op_code);
          break;
      }
    } else if (bytes_read == -1) {
      perror("read req_pipe");
    }
  }

  close(req_fd);
  close(notif_fd);
  return NULL;
}

void *client_listener(void *arg) {
        const char *register_pipe_path = (const char *)arg;
          struct sigaction sa;
          sa.sa_handler = handle_sigusr1;
          sigemptyset(&sa.sa_mask);
          sa.sa_flags = 0;
          sigaction(SIGUSR1, &sa, NULL);


        while (1) {
            int register_fd = open(register_pipe_path, O_RDONLY);
            if (register_fd == -1) {
                perror("open register_pipe");
                continue;
            }

            struct ClientData client_data_temp;
            memset(&client_data_temp, 0, sizeof(client_data_temp));

            char request_message[1 + 3 * MAX_PIPE_PATH_LENGTH];
            if (read(register_fd, request_message, sizeof(request_message)) == -1) {
                perror("read register_pipe");
                close(register_fd);
                continue;
            }

            // char op_code = request_message[0];
            strncpy(client_data_temp.req_pipe_path, request_message + 1, MAX_PIPE_PATH_LENGTH);
            strncpy(client_data_temp.resp_pipe_path, request_message + 1 + MAX_PIPE_PATH_LENGTH, MAX_PIPE_PATH_LENGTH);
            strncpy(client_data_temp.notif_pipe_path, request_message + 1 + 2 * MAX_PIPE_PATH_LENGTH, MAX_PIPE_PATH_LENGTH);

            pthread_mutex_lock(&clients_mutex);

            // Encontra um slot vazio ou reutiliza um cliente existente
            int client_id = -1;
            for (int i = 0; i < MAX_NUMBER_SUB; i++) {
                if (clients[i].active == 0) {
                    client_id = i;
                    break;
                }
            }

            if (client_id == -1) {
                fprintf(stderr, "Max clients reached. Cannot accept more clients.\n");
                pthread_mutex_unlock(&clients_mutex);
                close(register_fd);
                continue;
            }

            struct ClientData *client = &clients[client_id];

            int result;
            if (client_id == -1) {
                fprintf(stderr, "Max clients reached. Cannot accept more clients.\n");
                result = 1; // Erro ao adicionar cliente
            } else {
                if (!client->active) {
                    memset(client, 0, sizeof(struct ClientData));
                    strncpy(client->req_pipe_path, client_data_temp.req_pipe_path, MAX_PIPE_PATH_LENGTH);
                    strncpy(client->resp_pipe_path, client_data_temp.resp_pipe_path, MAX_PIPE_PATH_LENGTH);
                    strncpy(client->notif_pipe_path, client_data_temp.notif_pipe_path, MAX_PIPE_PATH_LENGTH);
                    client->active = 1;
                    result = 0; // Sucesso ao adicionar cliente
                } else {
                    result = 1; // Erro ao adicionar cliente
                }
            }

            pthread_mutex_unlock(&clients_mutex);

            int resp_fd = open(client_data_temp.resp_pipe_path, O_WRONLY);
            if (resp_fd != -1) {
                char response[2] = {OP_CODE_CONNECT, result};
                if (write(resp_fd, response, sizeof(response)) == -1) {
                    perror("write resp_pipe");
                }
                close(resp_fd);
            } else {
                perror("open resp_pipe");
            }

            // Adiciona o cliente à lista de clientes
            if (add_client(client) != 0) {
                fprintf(stderr, "Failed to add client\n");
                close(register_fd);
                continue;
            }

            // Cria uma thread para o cliente
            if (pthread_create(&client->thread, NULL, client_handler, client) != 0) {
                perror("pthread_create");
                pthread_mutex_lock(&clients_mutex);
                client->active = 0; // Marca o cliente como inativo em caso de erro
                pthread_mutex_unlock(&clients_mutex);
                close(register_fd);
                continue;
            }

            close(register_fd);
        }

        return NULL;
    }

void notify_clients(const char *key, const char *value) {
    pthread_mutex_lock(&clients_mutex);

    for (size_t i = 0; i < num_clients; i++) {
        for (int j = 0; j < clients[i].num_subscribed_keys; j++) {
            if (strcmp(clients[i].subscribed_keys[j], key) == 0) {
                char notification[MAX_STRING_SIZE * 2 + 3];
                snprintf(notification, sizeof(notification), "(%s,%s)\n", key, value);
                if (write(clients[i].notif_fd, notification, strlen(notification)) == -1) {
                    perror("Failed to write notification");
                }
            }
        }
    }

    pthread_mutex_unlock(&clients_mutex);
}


int main(int argc, char *argv[]) {
  if (argc < 5) {
    fprintf(stderr, "Usage: %s <jobs_directory> <max_threads> <backups_max> <register_fifo>\n", argv[0]);
    return 1;
  }

  jobs_directory = argv[1];
  max_threads = (size_t)atoi(argv[2]);
  max_backups = (size_t)atoi(argv[3]);
  const char *register_pipe_path = argv[4];

  // Inicializar o KVS
  kvs_table = kvs_init(); // Initialize kvs_table
  if (kvs_table == NULL) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }

  // Remover named pipe existente, se houver
  unlink(register_pipe_path);

  // Criar named pipe de registro
  if (mkfifo(register_pipe_path, 0666) == -1) {
    perror("mkfifo");
    return 1;
  }

  // Abrir o diretório de jobs
  DIR *dir = opendir(jobs_directory);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", jobs_directory);
    return 1;
  }

  // Criar thread para lidar com clientes
  pthread_t client_listener_thread;
  if (pthread_create(&client_listener_thread, NULL, client_listener, (void *)register_pipe_path) != 0) {
    perror("pthread_create");
    return 1;
  }

  // Criar thread para processar jobs
  pthread_t job_threads;
  if (pthread_create(&job_threads, NULL, (void *)dispatch_threads, (void *)dir) != 0) {
    perror("pthread_create");
    return 1;
  }



  // Esperar pelas threads
  pthread_join(client_listener_thread, NULL);
  pthread_join(job_threads, NULL);

  // Fechar o diretório de jobs
  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 1;
  }

  unlink(register_pipe_path);
  return 0;
}