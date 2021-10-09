#include "cacti.h"
#include <stdlib.h>
#include <stdio.h>
//#include <assert.h>

#define MSG_INFO 1
#define MSG_FACT 2

actor_id_t first;
int64_t n;

message_t msgGoDie = {
	.message_type = MSG_GODIE
};

typedef struct fact {
	int64_t k;
	int64_t res;
	int64_t n;
} fact_t;

role_t role;

message_t msgSpawn;

void hello(void** stateptr, size_t size, void* data) {
	(void)(size);
	(void)(stateptr);
	//printf("aktor %ld: dostałem hello od %ld", actor_id_self(), (actor_id_t)(data));
	message_t msg = {
		.message_type = MSG_INFO, //funkcja info
		.nbytes = sizeof(actor_id_self()),
		.data = (void*)actor_id_self()
	};
	if (actor_id_self() != first)
		send_message((actor_id_t)(data), msg); //wysyłam rodzicowi informacje o sobie
}

void info(void** stateptr, size_t size, void* data) { //wiadomość od dziecka - zakładam, że mam już silnię policzoną
	(void)(size);
	message_t msg = {
		.message_type = MSG_FACT,
		.nbytes = sizeof(*stateptr),
		.data = (void*)(*stateptr)
	};
	send_message((actor_id_t)(data), msg);
	send_message(actor_id_self(), msgGoDie);
}

void fact(void** stateptr, size_t size, void* data) {
	(void)(size);	
	*stateptr = (fact_t*)(data);
	((fact_t*)(*stateptr))->k++;
	((fact_t*)(*stateptr))->res *= ((fact_t*)(*stateptr))->k;

	//fprintf(stderr, "aktor %ld policzył silnię %d równą %d\n", actor_id_self(), ((fact_t*)(*stateptr))->k, ((fact_t*)(*stateptr))->res);

	if (((fact_t*)(*stateptr))->k < ((fact_t*)(*stateptr))->n) {
		send_message(actor_id_self(), msgSpawn); //każę sobie zespawnować nowego
	}
	else {
		printf("%ld\n", ((fact_t*)(*stateptr))->res);
		send_message(actor_id_self(), msgGoDie); //zabijam się
	}
	
}

int main(){
	
	scanf("%ld", &n);
	if (n < 0) {
		exit(1);
	}
	//assert(n >= 0);

	const size_t nprompts = 3;
    void (**prompts)(void**, size_t, void*) = malloc(sizeof(void*) * nprompts);
    prompts[MSG_HELLO] = &hello;
    prompts[MSG_INFO] = &info;
	prompts[MSG_FACT] = &fact;
    
	role.nprompts = nprompts;
	role.prompts = prompts;
	
	int res = actor_system_create(&first, &role);
	if (res != 0) {
		exit(1);
	}

	fact_t begin;
	begin.k = 0;
	begin.n = n;
	begin.res = 1;

	message_t msg = {
		.message_type = MSG_FACT,
		.nbytes = sizeof(fact_t*),
		.data = (void*)(&begin)
	};

	msgSpawn.message_type = MSG_SPAWN;
	msgSpawn.data = &role;

	send_message(first, msg);
	actor_system_join(first);
	free(prompts);
	//send_message(first, msgGoDie);
	//actor_system_join(first);
	//printf(*stateptr[2]);

	return 0;
}
