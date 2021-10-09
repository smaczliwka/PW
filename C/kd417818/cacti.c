#include "cacti.h"
#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <unistd.h>

//węzeł z id aktora
typedef struct node {
    actor_id_t val;
    struct node* next;
} node_t;

//tworzy węzeł z podanym id
node_t* new_node(actor_id_t sth) {
    node_t* new = (node_t*)malloc(sizeof(node_t));
    if (new == NULL)
        return NULL;
    new->val = sth;
    new->next = NULL;
    return new;
}

typedef struct queue {
    node_t* first;
    node_t* last;
    int len;
} queue_t;

queue_t* new_queue() {
    queue_t* new = (queue_t*)malloc(sizeof(queue_t));
    if (new == NULL)
        return NULL;
    new->first = new->last = NULL;
    new->len = 0;
    return new;
}

bool queue_add(queue_t* q, actor_id_t id) {
    node_t* new = new_node(id);
    if (new == NULL)
        return false;

    q->len++;

    if (q->last == NULL) {
        q->first = q->last = new;
        return true;
    }
    
    q->last->next = new;
    q->last = new;
    return true;
}

actor_id_t queue_get(queue_t* q) {
    if (q->first == NULL) {
        //assert(q->last == NULL);
        return -1;
    }
    
    q->len--;

    node_t* temp = q->first;
    q->first = q->first->next;

    if (q->first == NULL)
        q->last = NULL;

    actor_id_t id = temp->val;
    free(temp);
    return id;
}

bool queue_empty(queue_t* q) {
    if ((q->first == NULL || q->last == NULL || q->len == 0)) {
        //assert(q->first == NULL && q->last == NULL && q->len == 0);
        return true;
    }
    return false;
}

//węzeł z wiadomością
typedef struct mnode {
    message_t val;
    struct mnode* next;
} mnode_t;

//tworzy węzeł z podaną wiadomością
mnode_t* new_mnode(message_t sth) {
    mnode_t* new = (mnode_t*)malloc(sizeof(mnode_t));
    if (new == NULL)
        return NULL;
    new->val = sth;
    new->next = NULL;
    return new;
}

typedef struct mqueue {
    mnode_t* first;
    mnode_t* last;
    int len;
} mqueue_t;

mqueue_t* new_mqueue() {
    mqueue_t* new = (mqueue_t*)malloc(sizeof(mqueue_t));
    if (new == NULL)
        return NULL;
    new->first = new->last = NULL;
    new->len = 0;
    return new;
}

bool mqueue_add(mqueue_t* q, message_t message) {
    mnode_t* new = new_mnode(message);
    if (new == NULL)
        return false;

    q->len++;

    if (q->last == NULL) {
        q->first = q->last = new;
        return true;
    }
    
    q->last->next = new;
    q->last = new;
    return true;
}

message_t mqueue_get(mqueue_t* q) {
    //assert(q->first != NULL);

    q->len--;
    
    mnode_t* temp = q->first;
    q->first = q->first->next;

    if (q->first == NULL)
        q->last = NULL;

    message_t message = temp->val;
    free(temp);
    return message;
}

bool mqueue_empty(mqueue_t* q) {
    if (q->first == NULL || q->last == NULL || q->len == 0) {
        //assert(q->first == NULL && q->last == NULL && q->len == 0);
        return true;
    }
    return false;
}

typedef struct actor {

    //kolejka komunikatów
    mqueue_t* mailbox;

    //mutex dostępu do kolejki komunikatów
    pthread_mutex_t lock;

    role_t* role;
    actor_id_t id;

    bool dead; //czy jest martwy
    bool working; //czy jakiś wątek na nim działa

    void* state; //wskaźnik na stan tego aktora
} actor_t;

//tworzy nowego aktora i zwraca wskaźnik na niego
actor_t* new_actor(actor_id_t id, role_t* const role) {
    actor_t* actor = (actor_t*)malloc(sizeof(actor_t));

    if (actor == NULL)
        return NULL;

    actor->mailbox = new_mqueue();
    if (actor->mailbox == NULL) {
        free(actor);
        return NULL;
    }

    actor->role = role;
    actor->id = id;

    actor->dead = false;
    actor->working = false;

    actor->state = NULL;

    if (pthread_mutex_init(&actor->lock, 0) != 0) {
        free(actor->mailbox);
        free(actor);
        return NULL; //zwraca null jak się nie uda stworzyć mutexa
    }

    return actor;
}

typedef struct pool {

    pthread_t workers[POOL_SIZE];

    //tutaj wieszają się wątki w przypadku braku gotowych do działania aktorów
    pthread_cond_t passive;
    int passive_workers;

    //czy można skończyć
    //pthread_cond_t end;

    size_t dead_actors;

    //lista aktorów gotowych do działania
    queue_t* queue;

    //tablica wskaźników na aktorów
    actor_t** actors;
    size_t size;
    size_t number;

    //mutex pozwalający wątkowi na modyfikację listy
    pthread_mutex_t mutex;
    
} pool_t;

//zakładamy, że działa tylko jeden system jednocześnie
pool_t* global_pool;


actor_id_t add_actor(role_t* const role) {

    if (global_pool == NULL) {
        return -1;
    }

    //może być tak, że dwa wątki jednocześnie chcą stworzyć aktora
    pthread_mutex_lock(&global_pool->mutex);

    if (global_pool->number == CAST_LIMIT) {
        pthread_mutex_unlock(&global_pool->mutex);
        return -1;
    }

    if (global_pool->number == global_pool->size) {
        actor_t** new_actors = realloc(global_pool->actors, global_pool->size * 2 * sizeof(actor_t*));
        if (new_actors == NULL) {
            pthread_mutex_unlock(&global_pool->mutex);
            return -3; //nie udało się zaalokować pamięci
        }
        global_pool->actors = new_actors;
        global_pool->size *= 2;
    }

    if ((global_pool->actors[global_pool->number] = new_actor((actor_id_t)(global_pool->number), role)) == NULL) { 
        pthread_mutex_unlock(&global_pool->mutex);
        return -2; //nie udało się stworzyć aktora
    }
    actor_id_t retval = (actor_id_t)(global_pool->number);
    global_pool->number++;
    
    pthread_mutex_unlock(&global_pool->mutex);
    return retval; //zwraca id dodanego aktora
}

//lokalny dla każdego wątku numer aktualnie przetwarzanego aktora
__thread actor_id_t my_actor_id = -1;


void* work(void* data) { //argument to wskaźnik na indeks wątku w tablicy

    //size_t my_id = *((size_t*) data);
    free(data);

    //printf("NOWY WONTEK %ld\n", my_id);
    //actor_id_t* my_actor_id = malloc(sizeof(actor_id_t));
   
    while (true) {

        //bierze mutex od listy aktorów
        //printf("work %ld biore mutex\n", my_id);
        if (pthread_mutex_lock(&global_pool->mutex) != 0) {}
        //printf("work %ld wziąłem mutex\n", my_id);

        global_pool->passive_workers++;

        //if (global_pool->passive_workers == POOL_SIZE && queue_empty(global_pool->queue)) {
        if (global_pool->dead_actors == global_pool->number && global_pool->dead_actors != 0) {
            //fprintf(stderr, "wątek %ld wychodzi - wszyscy martwi - koniec pracy\n", my_id);
            pthread_cond_signal(&global_pool->passive);
            pthread_mutex_unlock(&global_pool->mutex);
            //free(my_actor_id);
            return NULL;
        }

        //node_t* my_actor; //node z wskaźnikiem na mojego aktora
        my_actor_id = queue_get(global_pool->queue);
        
        while (my_actor_id < 0 /*&& global_pool->working*/) {
            
            /* funkcja atomowo zwalnia mutex, który musiał być wcześniej w posiadaniu wątku 
            i zawiesza wątek na zmiennej warunkowej cond (chwilowe wyjście z monitora); 
            po obudzeniu wątek musi ponownie zdobyć mutex */
            
            //printf("brak aktora - work %ld wieszam sie\n", my_id);
            if (pthread_cond_wait(&global_pool->passive, &global_pool->mutex) != 0) {  }
            //printf("work %ld obudzony\n", my_id);

            //obudzono mnie i koniec pracy
            //if (global_pool->passive_workers == POOL_SIZE && queue_empty(global_pool->queue)) {
            if (global_pool->dead_actors == global_pool->number && global_pool->dead_actors != 0) {
                //fprintf(stderr, "wątek %ld obudzony - wszyscy martwi - koniec pracy\n", my_id);
                if (pthread_cond_signal(&global_pool->passive) != 0) { }
                pthread_mutex_unlock(&global_pool->mutex);
                //free(my_actor_id);
                return NULL;
            }
            
            my_actor_id = queue_get(global_pool->queue);
        }
        //fprintf(stderr, "wątek %ld bierze aktora %ld\n", my_id, my_actor_id);
        //assert(my_actor_id >= 0);
        //pthread_setspecific(currentActor, my_actor_id);
        //assert(actor_id_self() == my_actor_id);
        ////fprintf(stderr, "%ld\n", my_actor_id);

        //tutaj zakładam, że mam mutex od całęj puli
        //oraz że w kolejce aktorów ktoś jest

        // CZY BRAĆ MUTEX? KOLEJNOŚĆ!

        global_pool->passive_workers--;
        //assert(my_actor_id >= 0);
        actor_t* actor = global_pool->actors[my_actor_id];

        //biorę na chwilę mutex od tego aktora - nie wiem czy muszę
        pthread_mutex_lock(&actor->lock);
        //zaznaczam, że już nie jest w kolejce
        actor->working = true;
        //teraz to mój aktor
        //my_actor_id = actor->id;
        //zapamiętuje liczbę zadań do wykonania
        int tasks = actor->mailbox->len;
        //oddaję mutex od tego aktora
        pthread_mutex_unlock(&actor->lock);

        //oddaje mutex
        //printf("work %ld oddaje mutex przed działaniem\n", my_id);
        if (pthread_mutex_unlock(&global_pool->mutex) != 0) {}

        //działa z tym aktorem    
        
        //printf("worker %ld: mój aktor to %ld\n", my_id, actor->id);
        //assert(my_actor_id >= 0);

        for (int i = 0; i < tasks; i++) {
            message_t message;
            
            //biorę na chwilę mutex od tego aktora
            pthread_mutex_lock(&actor->lock);

            //zabieram komunikat z listy
            message = mqueue_get(actor->mailbox);

            if (message.message_type == MSG_GODIE) {
                actor->dead = true;
                //fprintf(stderr, "wątek %ld: aktor %ld przetwarza GO_DIE\n", my_id, my_actor_id);
            }

            //oddaję mutex od tego aktora
            pthread_mutex_unlock(&actor->lock);

            //printf("DZIAŁAM %ld\n", message.message_type);

            if (message.message_type != MSG_GODIE) {
                
                if (message.message_type == MSG_SPAWN) {
                    actor_id_t id = add_actor(message.data); //id tego, do którego wysyłam
                    // sprawdzić czy id nieujemne
                    message_t message;
                    message.message_type = MSG_HELLO;
                    message.data = (void*)(actor->id);
                    message.nbytes = sizeof(message.data);

                    send_message(id, message);
                }
                else {
                    //printf("message type %ld\n", message.message_type);
                    actor->role->prompts[message.message_type](&actor->state, message.nbytes, message.data);
                } 
            }
        }
        //assert(my_actor_id >= 0);
        //biorę na chwilę mutex od tego aktora
        pthread_mutex_lock(&actor->lock);

        //doszły nam jeszcze nowe wiadomości do przetworzenia
        if (!mqueue_empty(actor->mailbox)) {

            //fprintf(stderr, "wątek %ld wrzuca aktora %ld ponownie do kolejki\n", my_id, my_actor_id);

            //zabieram mutex od całej puli
            if (pthread_mutex_lock(&global_pool->mutex) != 0) {}

            //dodeję aktora do listy gotowych do działania
            //assert(my_actor_id >= 0);
            queue_add(global_pool->queue, my_actor_id);

            //budzę czekający ewentualnie wątek
            pthread_cond_signal(&global_pool->passive);

            //oddaję mutex od całej puli
            if (pthread_mutex_unlock(&global_pool->mutex) != 0) {}
        }
        else if (actor->dead) {
            //zabieram mutex od całej puli
            if (pthread_mutex_lock(&global_pool->mutex) != 0) {}

            global_pool->dead_actors++;
            //fprintf(stderr, "wątek %ld: aktor %ld dołącza do martwych, teraz %ld martwych, %ld wszystkich\n", my_id, actor_id_self(), global_pool->dead_actors, global_pool->number);
            pthread_cond_signal(&global_pool->passive);

            //oddaję mutex od całej puli
            if (pthread_mutex_unlock(&global_pool->mutex) != 0) {}
        }

        //a teraz już nie mam aktora
        my_actor_id = -1;
        //pthread_setspecific(currentActor, my_actor_id);
        actor->working = false;

        //oddaję mutex od tego aktora
        pthread_mutex_unlock(&actor->lock);
    }

    //free(my_actor_id);
    return NULL;
}

int actor_system_create(actor_id_t *actor, role_t *const role) {
    /*
    sigset_t block_mask, pending_mask;

    sigemptyset (&block_mask);
    sigaddset(&block_mask, SIGINT);                        //Blokuję SIGINT
    if (sigprocmask(SIG_BLOCK, &block_mask, 0) == -1)
        printf("sigprocmask block");
    */
   
    if (global_pool != NULL) {
        //printf("stary system istnieje\n");
        return -1;
    }

    global_pool = (pool_t*)malloc(sizeof(pool_t));
    if (global_pool == NULL)
        return -1; //nie udało się zaalokować pamięci
    
    if (pthread_cond_init(&global_pool->passive, 0) != 0) {
        free(global_pool);
        return -1; //nie udało się stworzyć zmiennej warunkowej
    }

    /*if (pthread_cond_init(&global_pool->end, 0) != 0)
        return -1; //nie udało się stworzyć zmiennej warunkowej*/

    global_pool->passive_workers = 0;
    global_pool->dead_actors = 0;

    //lista aktorów gotowych do działania (pusta)
    global_pool->queue = new_queue();

    //tworzy bufor aktorów
    global_pool->actors = (actor_t**)malloc(sizeof(actor_t*));
    if (global_pool->actors == NULL) {
        free(global_pool->queue);
        free(global_pool);
        return -3; //nie udało się stworzyć mutexa
    }
    global_pool->size = 1; //taka jest pojemność bufora
    global_pool->number = 0; //tylu aktorów jest - indeks następnego wstawianego

    if (pthread_mutex_init(&global_pool->mutex, 0) != 0) {
        free(global_pool->actors);
        free(global_pool->queue);
        free(global_pool);
        return -3; //nie udało się stworzyć mutexa
    }

    pthread_attr_t attr;
    if (pthread_attr_init (&attr) != 0) {
        free(global_pool->actors);
        free(global_pool->queue);
        free(global_pool);
        return -3; //nie udało się stworzyć mutexa
    }

    if (pthread_attr_setdetachstate (&attr,PTHREAD_CREATE_JOINABLE) != 0) {
        free(global_pool->actors);
        free(global_pool->queue);
        free(global_pool);
        return -3; //nie udało się stworzyć mutexa
    }

    for (int i = 0; i < POOL_SIZE; i++) {
        size_t* worker_arg = malloc(sizeof(size_t));
        *worker_arg = i;

        if (pthread_create(&global_pool->workers[i], &attr, work, worker_arg) != 0) {
            free(global_pool->actors);
            free(global_pool->queue);
            free(global_pool);
            return -7;
        }
    }

    //tworzy pierwszego aktora
    if (add_actor(role) < 0) {
        free(global_pool->actors);
        free(global_pool->queue);
        free(global_pool);
        return -7;
    }

    message_t message;
    message.message_type = MSG_HELLO;
    message.data = (void*)(-1);
    message.nbytes = sizeof(message.data);
    send_message(0, message);

    //zapisuje id pierwszego aktora
    *actor = (actor_id_t)(0);
    
    return 0;
}

void free_queue(queue_t* q) {
    while (!queue_empty(q))
        queue_get(q);
    free(q);
}

void free_mqueue(mqueue_t* q) {
    while (!mqueue_empty(q))
        mqueue_get(q);
    free(q);
}

void destroy_actor(actor_t* act) {
    if (act != NULL) {
        free_mqueue(act->mailbox);
        pthread_mutex_destroy(&act->lock);
    }
    free(act);
}

//zwraca coś niezerowego jak coś się wysypie
int actor_system_destroy() {
    int out = 0, err = 0;

    //global_pool->working = false; //to żeby wątki się w ogóle zakończyły    
    //pthread_cond_signal(&global_pool->passive);

    void* retval;
    for (int i = 0; i < POOL_SIZE; i++) {
        //printf("kończę wątek\n");
        if ((err = pthread_join(global_pool->workers[i], &retval)) != 0)
            out = err;
        //pthread_attr_destroy(global_pool->workers[i]);
    }

    //free(retval); //nie wiem po co to

    pthread_cond_destroy(&global_pool->passive);

    for (size_t i = 0; i < global_pool->number; i++) {
        destroy_actor(global_pool->actors[i]);
    }

    //czyści kolejkę gotowych
    free_queue(global_pool->queue);

    //czyści tablicę aktorów
    free(global_pool->actors);

    if ((err = pthread_mutex_destroy(&global_pool->mutex)) != 0)
        out = err;

    free(global_pool);
    global_pool = NULL;
    return out; //0 jeśli udało się zniczczyć wszystkie mutexy
}

void actor_system_join(actor_id_t actor) {
    if (global_pool == NULL || actor < 0 || (size_t)(actor) >= global_pool->number)
        return;

    actor_system_destroy(global_pool);
}

//zakładam, że w momencie wywoływania tego mam mutex???
int send_message(actor_id_t actor, message_t message) {
    //być może tutaj jeszcze trzeba zabrać mutex od całej puli

    //taki aktor nie istnieje
    if (global_pool == NULL || actor < 0 || global_pool->number <= (size_t)(actor))
        return -2;

    //zabieram mutex od tego aktora
    if (pthread_mutex_lock(&global_pool->actors[actor]->lock) != 0) {}

    //aktor jest martwy - nie odbiera komunikatów
    if (global_pool->actors[actor]->dead) {
        //zwalniam mutex od tego aktora
        if (pthread_mutex_unlock(&global_pool->actors[actor]->lock) != 0) {}
        return -1;
    }

    //aktor ma pełną kolejkę komunikatów
    if (global_pool->actors[actor]->mailbox->len == ACTOR_QUEUE_LIMIT) {
        //zwalniam mutex od tego aktora
        //fprintf(stderr, "kolejka aktora %ld przepełniona\n", actor);
        if (pthread_mutex_unlock(&global_pool->actors[actor]->lock) != 0) {}
        return -3;
    }

    //ten aktor miał pustą listę komunikatów
    if (mqueue_empty(global_pool->actors[actor]->mailbox)) {

        if (global_pool->actors[actor]->working == false) {

            //zabieram mutex od całej puli
            if (pthread_mutex_lock(&global_pool->mutex) != 0) {}

            //dodeję aktora do listy gotowych do działania   
            queue_add(global_pool->queue, actor);
            //fprintf(stderr, "send_message dodaje aktora %ld do kolejki\n", actor);
            //assert(global_pool->queue->last != NULL);

            //budzę czekający ewentualnie wątek
            pthread_cond_signal(&global_pool->passive);

            //oddaję mutex od całej puli
            if (pthread_mutex_unlock(&global_pool->mutex) != 0) {}    
        }
        
    }
    
    mqueue_add(global_pool->actors[actor]->mailbox, message);
    //assert(global_pool->actors[actor]->mailbox->last != NULL);
    
    //oddaję mutex od tego aktora   
    if (pthread_mutex_unlock(&global_pool->actors[actor]->lock) != 0) {}

    return 0;
}


actor_id_t actor_id_self() {
    return my_actor_id;
}

