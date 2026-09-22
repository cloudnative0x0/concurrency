# Mutex

<p style="text-align: left">
  <a href="#русский">Русский</a> ・ <a href="#english">English</a>
</p>

---

## Русский

`Mutex` защищает бизнес-инвариант, который нельзя безопасно изменить двумя горутинами одновременно. Он допускает в критическую секцию одного исполнителя, а остальных заставляет ждать. Защищать нужно не отдельную переменную, а всё правило целиком: прочитать текущее состояние, проверить условие и записать связанные изменения.

Реализация в этой папке учебная. Она показывает устройство mutex без использования `sync.Mutex`: атомарный fast path, очередь ожидающих, парковку горутин и прямую передачу владения. Для production-кода следует использовать стандартный `sync.Mutex`: он интегрирован с runtime, профилировщиком блокировок и race detector и содержит оптимизации, которые здесь намеренно не повторяются.

### Какую проблему решает mutex

Операция `counter++` состоит из чтения, вычисления и записи. Две горутины могут прочитать одно значение, обе вычислить следующий результат и затереть запись друг друга. Для бизнес-состояния последствия серьёзнее обычного неверного счётчика:

- две транзакции одновременно проходят проверку одного баланса и создают двойное списание;
- резерв товара становится отрицательным, хотя каждая заявка отдельно видела доступный остаток;
- сумма по счетам обновлена, а журнал операции ещё нет, поэтому состояние наблюдается наполовину применённым;
- map читается одновременно с записью и программа падает либо получает data race;
- одна горутина не обязана увидеть записи другой без отношения happens-before.

Mutex превращает последовательность «прочитать → проверить → изменить несколько полей» в одну критическую секцию. Второй исполнитель начинает её только после `Unlock` первого. Это одновременно даёт взаимоисключение и публикацию памяти: записи до `Unlock` становятся видимыми после последующего успешного `Lock`.

### Структура учебной реализации

```text
Mutex
├── state       atomic.Uint32  0 = свободен, 1 = занят
├── queueGuard  atomic.Uint32  короткий spin lock для списка ожидания
├── head        *waiter        первый ожидающий
└── tail        *waiter        последний ожидающий

waiter
├── ready       chan struct{}  персональный сигнал пробуждения
└── next        *waiter        следующий элемент FIFO-очереди
```

Здесь две разные блокировки с разной ролью. `state` определяет владельца пользовательского mutex. `queueGuard` защищает только несколько операций над `head` и `tail`; под ним не выполняется бизнес-код и горутина не ждёт освобождения основного mutex. Поэтому короткое активное ожидание через CAS и `runtime.Gosched` приемлемо именно для внутренней очереди, но не для произвольной критической секции.

`noCopy` ничего не блокирует во время исполнения. Его методы позволяют `go vet -copylocks` заметить копирование `Mutex` после первого использования. Копия имела бы отдельные атомарные поля, но разделяла или частично копировала бы очередь, что разрушило бы протокол владения.

### Lock: fast path и slow path

Сначала `Lock` выполняет `CompareAndSwap(0, 1)` над `state`. Если mutex свободен, CAS атомарно проверяет старое значение и записывает новое. Между проверкой и записью не существует окна, куда могла бы войти другая горутина. Это fast path: нет выделения памяти, канала и работы с очередью.

Если CAS не прошёл, начинается slow path:

1. Создаётся `waiter` с персональным каналом `ready`.
2. Горутина захватывает `queueGuard`.
3. CAS повторяется. Владелец мог вызвать `Unlock` между первой неудачей и захватом очереди; повторная проверка позволяет забрать уже свободный mutex и не уснуть навсегда.
4. Если mutex всё ещё занят, waiter добавляется в конец FIFO-очереди.
5. `queueGuard` освобождается, и горутина блокируется на `<-w.ready` без расходования CPU.

Персональный канал нужен не для передачи данных, а как одноразовое событие. Закрытие канала будит конкретного ожидающего и запоминается навсегда: если `Unlock` успел закрыть `ready` до начала receive, receive всё равно немедленно завершится. Это устраняет lost wake-up.

### Unlock и передача владения

`Unlock` кратко захватывает `queueGuard` и рассматривает два случая.

Если очередь пуста, `state.Store(0)` публикует свободное состояние. Следующий `Lock` заберёт mutex через fast path.

Если очередь не пуста, первый waiter удаляется, но `state` остаётся равным `1`. После освобождения `queueGuard` его канал `ready` закрывается. Разбуженная горутина возвращается из `Lock` уже владельцем; повторный CAS ей не нужен. Новая горутина не может вклиниться между прежним владельцем и waiter, потому что всё это время `state` не становился равным нулю.

Именно сохранение `state == 1` делает операцию handoff, а не обычным уведомлением. Если сначала записать `0`, а потом разбудить waiter, новая работающая горутина может выиграть CAS раньше уже ожидавшей. При постоянном потоке новых запросов старый waiter способен голодать.

Очередь FIFO означает, что после попадания в slow path горутины получают mutex по порядку. Это упрощает доказательство отсутствия starvation, но снижает пропускную способность при коротких критических секциях: разбуженной горутине может потребоваться переключение контекста, пока новый исполнитель уже работает на CPU.

### Чем отличается стандартный sync.Mutex

Стандартная реализация Go хранит в одном `int32` несколько частей состояния:

| Поле состояния | Назначение |
|---|---|
| `mutexLocked` | mutex занят |
| `mutexWoken` | один waiter уже разбужен; второго будить не нужно |
| `mutexStarving` | включена прямая передача владения |
| старшие биты | число ожидающих горутин |

Незанятый mutex также захватывается одним CAS. При конкуренции стандартная библиотека может коротко крутиться на CPU, затем паркует горутину через runtime-семафор.

В normal mode разбуженный waiter конкурирует с новыми горутинами. Новая горутина уже выполняется на CPU и часто выигрывает; это повышает throughput. Если waiter ждёт дольше примерно одной миллисекунды, mutex переходит в starvation mode. Тогда `Unlock` передаёт владение напрямую первому ожидающему, а новые горутины становятся в хвост. После сокращения очереди mutex возвращается в normal mode.

Учебная версия использует прямой FIFO handoff всегда, когда очередь не пуста. В ней нет `woken`/`starving`-битов, адаптивного spinning, runtime-семафора, mutex-профилирования и специальных hooks race detector. Алгоритм проще читать и проверять, но это не равноценная замена стандартной реализации по производительности и совместимости с будущими версиями Go.

### Почему видимость памяти гарантирована

Одного запрета на одновременный вход недостаточно: процессор и компилятор также должны согласовать видимость записей.

На свободном пути предыдущий владелец выполняет атомарный `state.Store(0)`, а следующий — успешный `CompareAndSwap(0, 1)`. Операции `sync/atomic` участвуют в едином последовательном порядке; CAS наблюдает опубликованное свободное состояние.

На пути handoff прежний владелец закрывает `ready`, а waiter возвращается после receive из этого канала. По memory model закрытие канала synchronizes-before receive, который видит закрытое состояние. Поэтому записи критической секции сделаны до того, как новый владелец продолжит работу.

Неудачный `TryLock` ничего не публикует и не даёт права читать защищённые данные.

### Блокчейн-пример: атомарное применение перевода

Один узел параллельно проверяет транзакции, но изменение локального ledger должно сохранить сразу несколько условий: отправитель не уходит в минус, сумма списывается один раз, получатель получает ту же сумму, комиссия попадает валидатору.

```go
var ErrInsufficientFunds = errors.New("insufficient funds")

type Transaction struct {
	From       string
	To         string
	AmountSats int64
	FeeSats    int64
}

type Ledger struct {
	mu       Mutex
	balances map[string]int64
}

func (l *Ledger) Apply(tx Transaction) error {
	totalDebit := tx.AmountSats + tx.FeeSats

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.balances[tx.From] < totalDebit {
		return ErrInsufficientFunds
	}

	l.balances[tx.From] -= totalDebit
	l.balances[tx.To] += tx.AmountSats
	l.balances["validator-fees"] += tx.FeeSats
	return nil
}
```

Проверка баланса и три записи находятся под одним mutex. Если Alice имеет 100 satoshi, две конкурентные транзакции по 80 не смогут обе увидеть исходные 100: вторая войдёт только после списания первой и получит `ErrInsufficientFunds`.

Проверку подписи, декодирование и сетевой запрос не следует выполнять под этим mutex — они не меняют `balances` и увеличивают время ожидания всех остальных транзакций. Снаружи выполняется дорогая независимая работа; внутри остаётся короткий commit защищённого состояния.

Этот mutex действует только внутри одного процесса. Он не синхронизирует разные блокчейн-узлы, не обеспечивает консенсус, не заменяет транзакцию базы данных и не защищает состояние после падения процесса.

### Операции

| Метод | Что делает | Блокируется |
|---|---|---|
| `Lock()` | получает исключительное владение | да, если mutex занят |
| `TryLock() bool` | пытается получить владение одним CAS | нет |
| `Unlock()` | освобождает mutex или передаёт его первому waiter | нет при корректном использовании |

Нулевое значение готово к работе; конструктор не нужен. `Unlock` незаблокированного mutex паникует. Mutex не реентерабелен: повторный `Lock` той же горутиной заблокируется, потому что Go mutex не хранит ID владельца.

### Практические правила

- Храните mutex рядом с данными, которые он защищает, и документируйте этот набор полей.
- Защищайте одну бизнес-операцию целиком, особенно связку check-then-act.
- Держите критическую секцию короткой; не выполняйте под lock сеть, диск и пользовательские callbacks.
- Сразу после `Lock` обычно ставьте `defer Unlock`, если критический участок не находится в очень горячем цикле.
- Не копируйте структуру с mutex после первого использования; передавайте указатель.
- Не вызывайте неизвестный код под lock: callback может попытаться взять тот же mutex и создать self-deadlock.
- Для нескольких mutex задайте единый порядок захвата. `A → B` в одном месте и `B → A` в другом создают deadlock.
- Не используйте `TryLock` как цикл с постоянным опросом. Это расходует CPU и обычно скрывает неправильную архитектуру ожидания.
- Запускайте `go vet -copylocks ./...` и `go test -race ./...`.

### Когда нужен другой инструмент

`Mutex` нужен для короткого исключительного доступа к общему состоянию. `RWMutex` может быть полезен при долгих чтениях и редких записях, но для коротких секций его дополнительные счётчики часто дороже. Atomics подходят для одного счётчика или флага, но плохо выражают инвариант между несколькими полями. Канал лучше, когда передаётся владение данными или моделируется поток команд. Для ограничения числа одновременных операций нужен semaphore, для ожидания завершения — `WaitGroup`, для одноразовой инициализации — `Once`.

### Официальные материалы

- [Исходник внутренней реализации Mutex](https://go.dev/src/internal/sync/mutex.go)
- [Документация sync.Mutex](https://pkg.go.dev/sync#Mutex)
- [Go Memory Model](https://go.dev/ref/mem)
- [Mutex или channel](https://go.dev/wiki/MutexOrChannel)

---

## English

`Mutex` protects a business invariant that two goroutines must not change at the same time. It admits one executor into a critical section and makes the rest wait. The protected unit is not necessarily one variable; it is the complete rule that reads current state, checks a condition, and updates related fields.

The implementation in this directory is educational. It demonstrates a mutex without using `sync.Mutex`: an atomic fast path, a waiter queue, goroutine parking, and direct ownership handoff. Production code should use the standard `sync.Mutex`, which is integrated with the runtime, mutex profiler, and race detector and contains optimizations intentionally omitted here.

### The problem a mutex solves

An operation such as `counter++` consists of a read, calculation, and write. Two goroutines can read the same value, calculate the same successor, and overwrite each other's result. Business state has more serious versions of the same race:

- two transactions both pass a balance check and create a double spend;
- inventory becomes negative even though each request observed available stock;
- account totals are updated while the operation journal is not, exposing a half-applied state;
- a map is read while another goroutine writes it, causing a race or runtime failure;
- one goroutine is not required to observe another's writes without a happens-before relation.

A mutex turns “read → validate → update several fields” into one critical section. A second executor starts it only after the first calls `Unlock`. This provides both exclusion and memory publication: writes before `Unlock` become visible after a later successful `Lock`.

### Structure of the educational implementation

```text
Mutex
├── state       atomic.Uint32  0 = free, 1 = owned
├── queueGuard  atomic.Uint32  short spin lock for the waiter list
├── head        *waiter        first waiter
└── tail        *waiter        last waiter

waiter
├── ready       chan struct{}  private wake-up signal
└── next        *waiter        next FIFO entry
```

The two locks have different jobs. `state` represents ownership of the user-facing mutex. `queueGuard` protects only a few pointer operations on `head` and `tail`; business code never runs under it, and no goroutine waits for user-level ownership while holding it. A short CAS loop with `runtime.Gosched` is reasonable for this internal queue, not for an arbitrary critical section.

`noCopy` performs no runtime locking. Its methods let `go vet -copylocks` report a `Mutex` copied after first use. A copy would have separate atomic fields while copying some queue pointers, breaking the ownership protocol.

### Lock: fast path and slow path

`Lock` starts with `CompareAndSwap(0, 1)` on `state`. If the mutex is free, CAS checks the old value and writes the new value atomically. No second goroutine can enter between the check and the write. This is the fast path: it allocates nothing and does not touch a channel or queue.

A failed CAS enters the slow path:

1. A `waiter` with a private `ready` channel is created.
2. The goroutine acquires `queueGuard`.
3. It repeats the CAS. The owner may have unlocked between the first failure and queue acquisition; this second check takes the now-free mutex instead of sleeping forever.
4. If the mutex is still owned, the waiter is appended to the FIFO queue.
5. `queueGuard` is released, and the goroutine blocks on `<-w.ready` without consuming CPU.

The private channel carries no data; it is a one-shot event. Closing it wakes that specific waiter and permanently records the event. If `Unlock` closes `ready` before the receive begins, the receive still completes immediately. This prevents a lost wake-up.

### Unlock and ownership handoff

`Unlock` briefly acquires `queueGuard` and handles two cases.

With no waiters, `state.Store(0)` publishes the free state. The next `Lock` takes it through the fast path.

With a non-empty queue, the first waiter is removed but `state` remains `1`. After releasing `queueGuard`, `Unlock` closes that waiter's `ready` channel. The awakened goroutine returns from `Lock` as the new owner; it does not need another CAS. A newly arriving goroutine cannot slip between the previous owner and the waiter because `state` never became zero.

Keeping `state == 1` is what makes this a handoff rather than a notification. If `Unlock` wrote `0` before waking the waiter, a new goroutine already running on a CPU could win the CAS first. Under a constant arrival rate, an old waiter could starve.

The FIFO queue means goroutines acquire the lock in queue order after entering the slow path. This makes starvation avoidance straightforward, but it can reduce throughput for tiny critical sections: a woken goroutine may require a context switch while a new goroutine is already running.

### How the standard sync.Mutex differs

Go's standard implementation packs several values into one `int32`:

| State field | Purpose |
|---|---|
| `mutexLocked` | the mutex is owned |
| `mutexWoken` | one waiter has already been woken |
| `mutexStarving` | direct handoff mode is active |
| high bits | number of waiting goroutines |

An uncontended lock is also one CAS. Under contention, the standard library may spin briefly before parking a goroutine on a runtime semaphore.

In normal mode, a woken waiter competes with newly arriving goroutines. A new goroutine is already executing on a CPU and often wins, improving throughput. If a waiter has been delayed for roughly one millisecond, the mutex switches to starvation mode. `Unlock` then hands ownership directly to the first waiter, and new arrivals join the tail. The mutex returns to normal mode after the queue pressure falls.

This educational version always uses direct FIFO handoff once a queue exists. It has no `woken` or `starving` bits, adaptive spinning, runtime semaphore, mutex profiling, or private race-detector hooks. It is easier to inspect and reason about, but it is not a performance or compatibility replacement for the standard library.

### Why memory becomes visible

Preventing simultaneous entry is not enough; the compiler and CPU must also agree on write visibility.

On the free path, the previous owner performs an atomic `state.Store(0)`, and the next owner succeeds with `CompareAndSwap(0, 1)`. Operations from `sync/atomic` participate in one sequentially consistent order, and the CAS observes the published free state.

On the handoff path, the previous owner closes `ready`, and the waiter continues after receiving from that channel. Under the Go memory model, closing a channel synchronizes before a receive that observes the closed state. Writes in the old critical section are therefore ordered before the new owner proceeds.

A failed `TryLock` has no synchronization effect and gives no right to read protected state.

### Blockchain example: applying a transfer atomically

A node may validate transactions concurrently, but changing its local ledger must preserve several conditions together: the sender cannot go negative, the amount is debited once, the receiver gets the same amount, and the validator receives the fee.

```go
var ErrInsufficientFunds = errors.New("insufficient funds")

type Transaction struct {
	From       string
	To         string
	AmountSats int64
	FeeSats    int64
}

type Ledger struct {
	mu       Mutex
	balances map[string]int64
}

func (l *Ledger) Apply(tx Transaction) error {
	totalDebit := tx.AmountSats + tx.FeeSats

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.balances[tx.From] < totalDebit {
		return ErrInsufficientFunds
	}

	l.balances[tx.From] -= totalDebit
	l.balances[tx.To] += tx.AmountSats
	l.balances["validator-fees"] += tx.FeeSats
	return nil
}
```

The balance check and all three writes use one mutex. If Alice owns 100 satoshi, two concurrent transfers of 80 cannot both observe the original 100: the second enters only after the first debit and returns `ErrInsufficientFunds`.

Signature verification, decoding, and network calls should stay outside this mutex. They do not modify `balances` and would extend the waiting time for every other transaction. Expensive independent work happens before the lock; only the short in-memory commit remains inside.

This mutex operates within one process. It does not synchronize separate blockchain nodes, provide consensus, replace a database transaction, or preserve state across a process crash.

### Operations

| Method | Behavior | Blocking |
|---|---|---|
| `Lock()` | obtains exclusive ownership | yes, while another owner exists |
| `TryLock() bool` | attempts ownership with one CAS | no |
| `Unlock()` | frees the mutex or hands it to the first waiter | no during correct use |

The zero value is ready for use; no constructor is needed. Unlocking an unlocked mutex panics. A mutex is not reentrant: a second `Lock` by the same goroutine blocks because Go mutexes do not store an owner goroutine ID.

### Practical rules

- Keep a mutex next to the data it protects and document that set of fields.
- Protect the complete business operation, especially check-then-act sequences.
- Keep critical sections short; do not perform network, disk, or user callback work while locked.
- Put `defer Unlock` immediately after `Lock` unless the section is inside an exceptionally hot loop.
- Never copy a struct containing a mutex after first use; pass a pointer.
- Do not call unknown code while locked: a callback can attempt the same lock and self-deadlock.
- Define one acquisition order for multiple mutexes. `A → B` in one path and `B → A` in another creates a deadlock.
- Do not turn `TryLock` into a polling loop. It wastes CPU and usually hides an incorrect waiting design.
- Run `go vet -copylocks ./...` and `go test -race ./...`.

### When another primitive fits better

Use `Mutex` for short exclusive access to shared state. `RWMutex` may help with long reads and rare writes, but its extra counters often cost more for short sections. Atomics work for one counter or flag but express multi-field invariants poorly. A channel fits ownership transfer or a command stream. Use a semaphore to limit concurrent operations, `WaitGroup` to await completion, and `Once` for one-time initialization.

### Official references

- [Internal Mutex implementation](https://go.dev/src/internal/sync/mutex.go)
- [sync.Mutex documentation](https://pkg.go.dev/sync#Mutex)
- [The Go Memory Model](https://go.dev/ref/mem)
- [Use a sync.Mutex or a channel?](https://go.dev/wiki/MutexOrChannel)
