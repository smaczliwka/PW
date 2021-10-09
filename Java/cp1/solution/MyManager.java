package cp1.solution;

import cp1.base.*;

import java.util.*;
import java.util.concurrent.*;

public class MyManager implements TransactionManager {

    private ConcurrentMap<ResourceId, Resource> resourceOfId; //resource o podanym Id
    private ConcurrentMap<ResourceId, Long> resourceOwner; //posiadacz resource o podanym Id
    private  ConcurrentMap<ResourceId, Semaphore> resourceAccess; //semafor chroniacy dostp do resource o podanym Id

    private ConcurrentMap<Thread, Stack<MyPair>> currentTransactionOperations;
    private ConcurrentMap<Thread, Long> startedTransaction; //<id watku, czas roczpoczecia>
    private ConcurrentMap<Thread, Boolean> abortedTransaction;
    private ConcurrentMap<Thread, Set<ResourceId>> myResources; //resources zajete przez ten thread

    private Semaphore threadDependencies;
    private ConcurrentMap<Thread, Thread> waitingForThread; // <watek czekajacy, na kogo czeka>

    private LocalTimeProvider timeProvider;

    private ConcurrentMap<Long, Thread> threadOfId;

    MyManager(Collection<Resource> resources,
                       LocalTimeProvider timeProvider) {

        this.timeProvider = timeProvider;
        this.resourceOwner = new ConcurrentHashMap<>();

        this.threadOfId = new ConcurrentHashMap<>();

        this.resourceOfId = new ConcurrentHashMap<>();
        this.resourceAccess = new ConcurrentHashMap<>();

        for (Resource r: resources) {
            resourceOfId.put(r.getId(), r);
            resourceOwner.put(r.getId(), new Long(-1)); //zaznaczam, ze nikt nie posiada danego zasobu na poczatku
            resourceAccess.put(r.getId(), new Semaphore(1, true));
        }

        this.currentTransactionOperations = new ConcurrentHashMap<>();
        this.startedTransaction = new ConcurrentHashMap<>();
        this.abortedTransaction = new ConcurrentHashMap<>(); //czy tranzakcja tego watku usunieta

        this.waitingForThread = new ConcurrentHashMap<>(); //Collections.synchronizedMap(new HashMap<>());
        this.threadDependencies = new Semaphore(1, true);

        this.myResources = new ConcurrentHashMap<>();

    }

    @Override
    public void startTransaction(
    ) throws
            AnotherTransactionActiveException {

        threadOfId.put(Thread.currentThread().getId(), Thread.currentThread()); //zaznaczam swoje istnienie

        if (startedTransaction.containsKey(Thread.currentThread())) {
            throw new AnotherTransactionActiveException();
        }
        startedTransaction.put(Thread.currentThread(), timeProvider.getTime());
    }

    private Thread toAbort(Thread start) {
        Thread candidate = null;
        Thread t = start;
        while (waitingForThread.containsKey(t)) {
            if (candidate == null || startedTransaction.get(candidate) < startedTransaction.get(t)
              || (startedTransaction.get(candidate).equals(startedTransaction.get(t)) && candidate.getId() < t.getId())) {
                candidate = t;
            }
            t = waitingForThread.get(t);

            if (t.getId() == start.getId()) {

                //na pewno znaleziono jakiegos do abortowania
                abortedTransaction.put(candidate, true);
                candidate.interrupt();
                waitingForThread.remove(candidate); //abortowany na nikogo nie czeka

                return candidate;
            }
        }
        return null;
    }

    @Override
    public void operateOnResourceInCurrentTransaction(
            ResourceId rid,
            ResourceOperation operation
    ) throws
            NoActiveTransactionException,
            UnknownResourceIdException,
            ActiveTransactionAborted,
            ResourceOperationException,
            InterruptedException {

        if (!startedTransaction.containsKey(Thread.currentThread())) {
            throw new NoActiveTransactionException();
        }

        if (abortedTransaction.containsKey(Thread.currentThread())) {
            throw new ActiveTransactionAborted();
        }

        if (!resourceOfId.containsKey(rid)) {
            throw new UnknownResourceIdException(rid);
        }

        synchronized (resourceOwner.get(rid)) { //wieszam sie na aktualnym posiadaczu tego zasobu

            if (resourceOwner.get(rid) == Thread.currentThread().getId()) { //jesli mialem go wczesniej
            }

            else if (resourceOwner.get(rid) == -1) { //zasob jest wolny

                resourceAccess.get(rid).acquire(); //na pewno sie nie zawiesze - zabieram dostep

                resourceOwner.put(rid, Thread.currentThread().getId());
                myResources.putIfAbsent(Thread.currentThread(), new HashSet<>()); //wstawiam pusty set jesli to moja pierwszy
                myResources.get(Thread.currentThread()).add(rid);
            }

            else  { //jesli ma go ktos inny

                try {
                    threadDependencies.acquire(); //zakladam zamek na grafie zaleznosci i sprawdzam, czy cykl
                } catch (InterruptedException e) { //nikt mnie tu nie zabortuje na pewno
                    Thread.currentThread().interrupt();
                    throw e; //niedodany do grafu, wiec nikt mnie nie abortowal
                }

                if (resourceOwner.get(rid) != -1) { //jesli sie nie zwolnilo podczas czekania

                    waitingForThread.put(Thread.currentThread(), threadOfId.get(resourceOwner.get(rid))); //zaznaczam, ze czekam na tego
                    //nadal mam graf na wylacznosc
                    toAbort(Thread.currentThread());
                }
                else {
                    resourceAccess.get(rid).acquire(); //na pewno sie nie zawiesze - zabieram dostep

                    resourceOwner.put(rid, Thread.currentThread().getId());
                    myResources.putIfAbsent(Thread.currentThread(), new HashSet<>()); //wstawiam pusty set jesli to moja pierwszy
                    myResources.get(Thread.currentThread()).add(rid);
                }

                threadDependencies.release(); //oddaje graf zaleznosci

                if (Thread.interrupted()) {
                    Thread.currentThread().interrupt();
                    if (abortedTransaction.containsKey(Thread.currentThread())) {

                        throw new ActiveTransactionAborted();
                    }
                    throw new InterruptedException();
                }
            }
        }

        if (!myResources.getOrDefault(Thread.currentThread(), new HashSet<>()).contains(rid)) { //jesli zasob nie byl moj wczesniej, probuje go dostac
            try {

                resourceAccess.get(rid).acquire(); //wieszam sie na semaforze tego resource

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (abortedTransaction.containsKey(Thread.currentThread())) { //jesli sam siebie abortowalem, to i tak zadziala

                    throw new ActiveTransactionAborted();
                }
                throw e;
            }

            resourceOwner.put(rid, Thread.currentThread().getId());
            myResources.putIfAbsent(Thread.currentThread(), new HashSet<>()); //wstawiam pusty set jesli to moj pierwszy
            myResources.get(Thread.currentThread()).add(rid);
        }

        threadDependencies.acquire();
        waitingForThread.remove(Thread.currentThread()); //juz na nikogo nie czekam jak mnie obudzono
        threadDependencies.release();

        try {
            operation.execute(resourceOfId.get(rid));
        } catch (ResourceOperationException e) { //wydaje mi sie, ze nie chcemy wtedy blokowac zasobu
            throw e;
        }

        currentTransactionOperations.putIfAbsent(Thread.currentThread(), new Stack<>()); //wstawiam pusty stos jesli pierwsza tranzakcja
        currentTransactionOperations.get(Thread.currentThread()).push(new MyPair(operation, resourceOfId.get(rid)));
    }

    @Override
    public void commitCurrentTransaction(
    ) throws
            NoActiveTransactionException,
            ActiveTransactionAborted {
        if (!startedTransaction.containsKey(Thread.currentThread())) {
            throw new NoActiveTransactionException();
        }
        if (abortedTransaction.containsKey(Thread.currentThread())) {
            throw new ActiveTransactionAborted();
        }

        if (myResources.containsKey(Thread.currentThread())) {
            for (ResourceId rid: myResources.get(Thread.currentThread())) {
                resourceOwner.replace(rid, new Long(-1));

                resourceAccess.get(rid).release(); //budze jakiegos czekajacego na moj
            }
            myResources.remove(Thread.currentThread());
        }

        currentTransactionOperations.remove(Thread.currentThread());
        startedTransaction.remove(Thread.currentThread());
    }

    @Override
    public void rollbackCurrentTransaction() {

        if (currentTransactionOperations.containsKey(Thread.currentThread())) {
            while (!currentTransactionOperations.get(Thread.currentThread()).empty()) {
                MyPair last = currentTransactionOperations.get(Thread.currentThread()).pop();
                last.op().undo(last.res());
            }
            currentTransactionOperations.remove(Thread.currentThread());
        }

        // teraz trzeba odblokowac watki czekajace na zasobach
        if (myResources.containsKey(Thread.currentThread())) {
            for (ResourceId rid: myResources.get(Thread.currentThread())) {
                resourceOwner.replace(rid, new Long(-1));

                resourceAccess.get(rid).release(); //budze jakiegos czekajacego na moj
            }
            myResources.remove(Thread.currentThread());
        }

        startedTransaction.remove(Thread.currentThread());
        abortedTransaction.remove(Thread.currentThread());
    }

    @Override
    public boolean isTransactionActive() {
        return startedTransaction.containsKey(Thread.currentThread());
    }

    @Override
    public boolean isTransactionAborted() {
        return abortedTransaction.containsKey(Thread.currentThread());
    }
}
