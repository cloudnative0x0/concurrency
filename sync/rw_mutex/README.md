# RWMutex

<p style="text-align: left">
  <a href="#русский">Русский</a> ・ <a href="#english">English</a>
</p>

---

## Русский

`RWMutex` разделяет доступ к общему состоянию на два режима:

- несколько горутин могут одновременно держать `RLock`, если они только читают;
- `Lock` даёт одному writer исключительный доступ: в этот момент нет ни других writers, ни readers.

Практический смысл появляется не от большого числа горутин, а от конкретного профиля нагрузки. Если чтения заметно дольше самого захвата lock, выполняются часто и действительно не меняют состояние, их параллельное выполнение может увеличить пропускную способность. При коротких чтениях или частых записях обычный `Mutex` нередко дешевле.

Реализация в этой папке учебная и не использует `sync.RWMutex`. Она показывает счётчик активных readers, отдельные FIFO-очереди readers и writers, writer priority, групповое пробуждение reader-фазы и прямой handoff writer. Для production-кода нужен стандартный `sync.RWMutex`: он использует runtime-семафоры, более дешёвый reader fast path, профилирование блокировок и специальные hooks race detector.

### Какие проблемы решает RWMutex

RWMutex нужен, когда состояние должно удовлетворять инварианту, writers меняют его атомарно, а readers должны видеть один завершённый вариант — до изменения или после него, но не середину.

Без блокировки возникают разные классы ошибок:

- reader видит новый `height`, но старые balances, потому что применение блока ещё не закончено;
- два writers одновременно изменяют map и повреждают бизнес-состояние;
- проверка и последующая запись работают с разными версиями данных;
- concurrent map read/write приводит к runtime failure;
- данные формально записаны, но между горутинами нет happens-before, гарантирующего их видимость.

Обычный `Mutex` также решает эти проблемы. Дополнительная возможность RWMutex — разрешить нескольким read-only критическим секциям выполняться вместе.

### Структура учебной реализации

```text
RWMutex
├── gate            atomic.Uint32  внутренний короткий spin lock
├── activeReaders   uint32         число holders RLock
├── writerActive    bool           есть holder Lock
├── readerHead/Tail *waiter        FIFO readers
├── readerQueued    uint32         readers в ожидании
├── writerHead/Tail *waiter        FIFO writers
└── writerQueued    uint32         writers в ожидании

waiter
├── ready           chan struct{}  персональное событие пробуждения
└── next            *waiter        следующий элемент очереди
```

`gate` — не пользовательская блокировка. Он защищает только внутренние счётчики и указатели очередей. Под ним выполняется несколько присваиваний; ожидание `RLock` или `Lock` всегда происходит после освобождения gate. Это критично: заснувшая с gate горутина не позволила бы `RUnlock` или `Unlock` изменить состояние и разбудить её.

Gate построен на CAS. При краткой внутренней конкуренции проигравшая горутина вызывает `runtime.Gosched`, уступая выполнение. Такой spin допустим для нескольких инструкций управления очередью, но не для сетевого запроса, обращения к диску или бизнес-вычисления.

Каждый waiter получает собственный канал `ready`. Закрытие канала — одноразовый сигнал, который нельзя потерять: receive завершится, даже если начался после `close`. Один канал на waiter также исключает пробуждение не той категории участника.

### RLock и RUnlock

`RLock` берёт gate и проверяет два условия:

```text
writerActive == false
writerQueued == 0
```

Если оба выполнены, `activeReaders` увеличивается и горутина входит в read-секцию. Несколько readers проходят эту проверку по очереди под коротким gate, но их защищённая работа затем выполняется параллельно.

Проверка `writerQueued == 0` предотвращает starvation writer. Как только writer встал в очередь, новые readers больше не присоединяются к текущей reader-фазе. Иначе постоянный поток коротких чтений мог бы никогда не довести `activeReaders` до нуля.

Если reader войти не может, он добавляется в reader queue, освобождает gate и паркуется на своём `ready`.

`RUnlock` уменьшает `activeReaders`. Обычный reader после этого сразу выходит. Последний reader, если есть ожидающий writer, удаляет его из начала очереди, выставляет `writerActive = true`, освобождает gate и закрывает канал writer. Флаг устанавливается до пробуждения, поэтому новый reader не может вклиниться в окно handoff.

### Lock и Unlock

`Lock` получает владение сразу, если нет активного writer и `activeReaders == 0`. Иначе writer ставится в FIFO-очередь и засыпает.

`Unlock` выбирает следующую фазу:

1. Если за writer накопились readers, вся reader queue отделяется от общей структуры. `activeReaders` заранее устанавливается в число этих waiters, `writerActive` сбрасывается, затем все их каналы закрываются.
2. Если readers не ждут, но есть writers, владение напрямую передаётся первому writer. `writerActive` между владельцами не сбрасывается.
3. Если никто не ждёт, состояние становится свободным.

Readers пробуждаются группой: именно это даёт параллелизм чтения. Если одновременно ожидают и readers, и writers, после writer запускается накопленная reader-фаза. Новые readers при этом видят `writerQueued > 0` и становятся уже в следующую очередь. Последний reader текущей группы передаёт lock writer. Получается чередование фаз:

```text
writer → queued readers together → next writer → queued readers together
```

Так writer не голодает под непрерывными новыми чтениями, а readers не остаются навсегда за непрерывной очередью writers.

### Где находится точка линеаризации

Все решения о смене владельца принимаются под одним gate. Это важнее самого выбора атомарных типов.

Опасная реализация может хранить reader count и writer pending в разных atomic-переходах. Тогда последний reader способен уйти после первой проверки writer, но до публикации pending. Reader не увидит, кого нужно разбудить, а writer после публикации уснёт — оба события по отдельности корректны, но wake-up потерян.

В этой реализации постановка в очередь, проверка счётчиков и выбор следующей фазы сериализованы gate. Участник либо видит свободное состояние и становится владельцем, либо сначала публикует waiter в очереди, которую последующий unlock уже не может не заметить.

### Как устроен sync.RWMutex в Go

Стандартная реализация компактнее и быстрее. В ней есть:

| Поле | Роль |
|---|---|
| `w Mutex` | сериализует конкурирующих writers |
| `writerSem` | паркует writer до ухода активных readers |
| `readerSem` | паркует readers за ожидающим writer |
| `readerCount` | число readers или отрицательный признак pending writer |
| `readerWait` | сколько прежних readers ещё должен дождаться writer |

Reader fast path делает атомарное увеличение `readerCount`. Writer сначала захватывает `w`, затем вычитает большое значение `rwmutexMaxReaders`. Отрицательный счётчик одновременно сообщает новым readers, что writer ожидает, и сохраняет число уже активных readers. Последний из них освобождает `writerSem`. При `Unlock` writer возвращает смещение и будит readers через `readerSem`.

Учебная версия выражает те же роли явными полями и очередями. Она проще для пошагового разбора, но каждый `RLock` кратко сериализуется на gate, waiter создаёт канал, а интеграции с runtime mutex profile нет. Сравнивать её benchmark с `sync.RWMutex` полезно для обучения, но подменять стандартный тип в production нельзя.

### Видимость памяти

Writer меняет защищённые данные до `Unlock`. Следующая фаза получает сигнал после этих записей:

- ожидающий writer продолжает после закрытия его `ready`;
- ожидающие readers продолжают после закрытия своих `ready`;
- новый участник проходит через атомарный gate после того, как прежний владелец опубликовал свободное состояние.

Закрытие канала synchronizes-before receive, который наблюдает закрытие. Атомарные операции gate образуют последовательный порядок. Поэтому новый holder видит завершённые записи предыдущего writer.

Readers не синхронизируют произвольные записи друг друга. Код под `RLock` обязан только читать защищённое состояние. Запись под read lock остаётся ошибкой и может конфликтовать с другим reader.

### Блокчейн-пример: snapshot и применение блока

API узла часто читает баланс и текущую высоту, а отдельная горутина применяет финализированный блок. Все изменения блока должны стать видимыми одной версией.

```go
type Balance struct {
	Amount int64
	Height uint64
}

type Transfer struct {
	From   string
	To     string
	Amount int64
}

type ChainState struct {
	mu       RWMutex
	height   uint64
	balances map[string]int64
}

func (s *ChainState) Balance(address string) Balance {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return Balance{
		Amount: s.balances[address],
		Height: s.height,
	}
}

func (s *ChainState) ApplyBlock(height uint64, transfers []Transfer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, tx := range transfers {
		s.balances[tx.From] -= tx.Amount
		s.balances[tx.To] += tx.Amount
	}
	s.height = height
}
```

Несколько запросов `Balance` могут выполняться одновременно. `ApplyBlock` ждёт текущие чтения; после постановки writer в очередь новые запросы не обходят его бесконечно. Во время применения блока ни один reader не увидит часть переводов вместе с новой высотой. После `Unlock` следующий reader увидит завершённый state.

Проверку подписи, загрузку блока и обращения к базе следует сделать до `Lock`. Под write lock остаётся короткий commit уже проверенного набора изменений. Если удерживать его во время RPC, один медленный peer остановит и обновление цепочки, и все read-запросы.

RWMutex действует только в памяти одного процесса. Он не обеспечивает консенсус между узлами, транзакционность постоянного хранилища, откат reorg или восстановление после сбоя.

### Операции

| Метод | Назначение |
|---|---|
| `RLock()` | ждёт и получает совместное read-владение |
| `TryRLock() bool` | пытается получить read-владение без ожидания |
| `RUnlock()` | освобождает одно read-владение |
| `Lock()` | ждёт и получает исключительное write-владение |
| `TryLock() bool` | пытается получить write-владение без ожидания |
| `Unlock()` | освобождает write-владение и выбирает следующую фазу |
| `RLocker()` | возвращает `sync.Locker`, использующий `RLock`/`RUnlock` |

Нулевое значение готово к работе. `RUnlock` без `RLock` и `Unlock` без `Lock` паникуют. Неудачный Try-метод не создаёт happens-before и не разрешает читать защищённые данные. В учебной реализации Try-метод также может вернуть `false`, если внутренний gate кратко занят.

### Ограничения и ошибки использования

RWMutex не реентерабелен. Рекурсивный `RLock` кажется рабочим, пока нет writer. Если writer уже ждёт, второй `RLock` блокируется, а writer ждёт освобождения первого — получается deadlock.

Нельзя безопасно сделать upgrade `RLock → Lock`: gorутина остаётся в числе active readers и сама ждёт их обнуления. Нельзя считать атомарным downgrade `Lock → RLock`: между `Unlock` и `RLock` может пройти другой writer.

Как и обычный mutex, RWMutex не хранит ID владельца. Технически одна горутина может освободить lock, взятый другой, но такой handoff должен быть явной частью протокола. Случайный cross-goroutine unlock разрушает бизнес-инвариант.

Не копируйте RWMutex после первого использования. `noCopy` помогает `go vet -copylocks` обнаружить это статически.

### Когда использовать

RWMutex имеет смысл, когда одновременно выполняются условия:

- чтений существенно больше, чем записей;
- read-секция достаточно дорогая, чтобы параллелизм окупал координацию;
- readers не изменяют защищённые данные, включая map, slice и объекты по указателям;
- допустимо остановить новые reads, когда writer ждёт.

Для короткого чтения одного поля сначала измерьте обычный `Mutex` или `atomic`. При частых writes reader-фазы постоянно закрываются, очереди пробуждаются, и RWMutex становится дороже. Для immutable snapshot удобно публиковать готовую копию через `atomic.Pointer`. Канал подходит, если состоянием владеет одна goroutine и остальные отправляют ей команды.

Всегда проверяйте конкретную нагрузку benchmark'ом, а корректность — `go test -race ./...` и `go vet -copylocks ./...`.

### Официальные материалы

- [Исходник sync.RWMutex](https://go.dev/src/sync/rwmutex.go)
- [Документация sync.RWMutex](https://pkg.go.dev/sync#RWMutex)
- [Go Memory Model](https://go.dev/ref/mem)

---

## English

`RWMutex` divides shared-state access into two modes:

- any number of goroutines may hold `RLock` together while they only read;
- `Lock` gives one writer exclusive access, with no other writer or reader active.

The benefit comes from a specific workload, not merely from having many goroutines. Parallel reads help when they occur frequently, take noticeably longer than lock acquisition, and truly do not mutate state. A regular `Mutex` is often cheaper for tiny reads or frequent writes.

The implementation in this directory is educational and does not use `sync.RWMutex`. It exposes an active-reader count, separate FIFO queues for readers and writers, writer priority, batched reader phases, and direct writer handoff. Production code should use the standard `sync.RWMutex`, which has runtime semaphores, a cheaper reader fast path, lock profiling, and private race-detector hooks.

### Problems solved by RWMutex

RWMutex applies when state must preserve an invariant, writers change it atomically, and readers must observe one completed version—before or after a change, never halfway through it.

Without synchronization:

- a reader can observe a new block height with old balances while block application is incomplete;
- two writers can modify a map and corrupt business state;
- validation and its following write can act on different versions;
- concurrent map read and write can fail at runtime;
- writes need a happens-before relation before another goroutine is guaranteed to observe them.

A regular `Mutex` solves these problems too. RWMutex additionally allows multiple read-only critical sections to execute together.

### Structure of the educational implementation

```text
RWMutex
├── gate            atomic.Uint32  short internal spin lock
├── activeReaders   uint32         current RLock holders
├── writerActive    bool           current Lock holder exists
├── readerHead/Tail *waiter        reader FIFO
├── readerQueued    uint32         waiting readers
├── writerHead/Tail *waiter        writer FIFO
└── writerQueued    uint32         waiting writers

waiter
├── ready           chan struct{}  private wake-up event
└── next            *waiter        next queue entry
```

`gate` is not the user-facing lock. It protects only internal counters and queue pointers. It is held for a few assignments; a goroutine always releases it before waiting in `RLock` or `Lock`. Parking while holding gate would prevent `RUnlock` or `Unlock` from changing state and waking that goroutine.

Gate uses CAS. A loser under brief internal contention calls `runtime.Gosched` to yield execution. This spin is acceptable around a handful of queue operations, not around network, disk, or business work.

Every waiter has a private `ready` channel. Closing it is a one-shot event that cannot be lost: a receive still completes if it begins after `close`. Private channels also prevent waking a participant from the wrong category.

### RLock and RUnlock

`RLock` takes gate and checks:

```text
writerActive == false
writerQueued == 0
```

If both conditions hold, it increments `activeReaders` and enters the read section. Readers pass this short gate one at a time, but their protected work then runs concurrently.

Checking `writerQueued == 0` prevents writer starvation. Once a writer queues, new readers stop joining the current reader phase. Otherwise a constant stream of short reads could prevent `activeReaders` from ever reaching zero.

A reader that cannot enter joins the reader queue, releases gate, and parks on its `ready` channel.

`RUnlock` decrements `activeReaders`. An ordinary reader then returns. If the final reader finds a waiting writer, it removes the oldest writer, sets `writerActive = true`, releases gate, and closes the writer's channel. Setting the flag before wake-up prevents a new reader from entering during handoff.

### Lock and Unlock

`Lock` takes ownership immediately when there is no active writer and `activeReaders == 0`. Otherwise it queues the writer in FIFO order and parks it.

`Unlock` chooses the next phase:

1. If readers accumulated behind the writer, the entire reader queue is detached. `activeReaders` is set to the number of waiters and `writerActive` is cleared before all reader channels are closed.
2. If no readers wait but writers do, ownership is handed directly to the oldest writer. `writerActive` stays true across the transition.
3. If nobody waits, the state becomes free.

Readers wake as a batch, which is where read parallelism comes from. When both readers and writers wait, the accumulated reader phase follows the current writer. New readers still observe `writerQueued > 0` and join the next queue. The final reader in the released group hands ownership to the next writer:

```text
writer → queued readers together → next writer → queued readers together
```

This prevents continuous new readers from starving writers and prevents a continuous writer queue from starving readers already waiting.

### The linearization point

All ownership decisions happen under one gate. This matters more than merely choosing atomic field types.

A broken design may publish the reader count and writer-pending flag in separate atomic transitions. The final reader can leave after a writer's first check but before it publishes pending state. That reader sees nobody to wake, while the writer publishes its flag and sleeps. Both individual operations are atomic, but the wake-up is lost.

Here, queue insertion, counter checks, and next-phase selection are serialized by gate. A participant either observes free state and becomes the owner or publishes a waiter that a later unlock cannot miss.

### How Go's sync.RWMutex works

The standard implementation is more compact and faster. Its main fields are:

| Field | Role |
|---|---|
| `w Mutex` | serializes competing writers |
| `writerSem` | parks a writer until active readers leave |
| `readerSem` | parks readers behind a waiting writer |
| `readerCount` | active readers or a negative pending-writer marker |
| `readerWait` | number of previous readers the writer still awaits |

The reader fast path atomically increments `readerCount`. A writer first acquires `w`, then subtracts the large `rwmutexMaxReaders` constant. A negative count both blocks new readers and preserves the number of readers already active. The final old reader releases `writerSem`. On `Unlock`, the writer restores the offset and wakes readers through `readerSem`.

The educational version expresses the same roles with explicit fields and queues. It is easier to trace, but every `RLock` briefly serializes on gate, each waiter allocates a channel, and there is no runtime mutex profiling. Benchmarking it against `sync.RWMutex` is useful for study; replacing the standard type in production is not.

### Memory visibility

A writer changes protected data before `Unlock`. The next phase receives a signal after those writes:

- a waiting writer continues after its `ready` channel closes;
- waiting readers continue after their channels close;
- a new participant passes through the atomic gate after the former owner published free state.

Closing a channel synchronizes before a receive that observes the close. Atomic gate operations form a sequentially consistent order. A new holder therefore observes completed writes from the previous writer.

Readers do not synchronize arbitrary writes with each other. Code under `RLock` must only read protected state. Writing while holding a read lock is still a bug and can race with another reader.

### Blockchain example: snapshots and block application

A node API often reads balances and current height while another goroutine applies a finalized block. Every change from the block must become visible as one version.

```go
type Balance struct {
	Amount int64
	Height uint64
}

type Transfer struct {
	From   string
	To     string
	Amount int64
}

type ChainState struct {
	mu       RWMutex
	height   uint64
	balances map[string]int64
}

func (s *ChainState) Balance(address string) Balance {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return Balance{
		Amount: s.balances[address],
		Height: s.height,
	}
}

func (s *ChainState) ApplyBlock(height uint64, transfers []Transfer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, tx := range transfers {
		s.balances[tx.From] -= tx.Amount
		s.balances[tx.To] += tx.Amount
	}
	s.height = height
}
```

Several `Balance` calls may execute together. `ApplyBlock` waits for current reads, and new requests stop bypassing it once the writer queues. No reader sees a subset of transfers paired with the new height. Readers entering after `Unlock` observe the completed state.

Signature verification, block download, and database calls should happen before `Lock`. Only the short commit of already validated changes belongs under the write lock. Holding it during RPC lets one slow peer stop both chain updates and all read requests.

RWMutex operates only in one process. It does not provide node consensus, durable-storage transactions, reorg rollback, or crash recovery.

### Operations

| Method | Purpose |
|---|---|
| `RLock()` | waits for and acquires shared read ownership |
| `TryRLock() bool` | attempts read ownership without waiting |
| `RUnlock()` | releases one read ownership |
| `Lock()` | waits for and acquires exclusive write ownership |
| `TryLock() bool` | attempts write ownership without waiting |
| `Unlock()` | releases write ownership and selects the next phase |
| `RLocker()` | returns a `sync.Locker` backed by `RLock`/`RUnlock` |

The zero value is ready for use. `RUnlock` without `RLock` and `Unlock` without `Lock` panic. A failed Try method creates no happens-before relation and grants no access to protected data. In this educational implementation, a Try method may also return `false` while the short internal gate is busy.

### Limitations and misuse

RWMutex is not reentrant. Recursive `RLock` appears to work until a writer waits. The second `RLock` then blocks, while the writer waits for the first one to be released, producing a deadlock.

An `RLock → Lock` upgrade is unsafe: the goroutine remains one of the active readers while waiting for that same count to reach zero. `Lock → RLock` is not an atomic downgrade either; another writer may enter between `Unlock` and `RLock`.

Like a regular mutex, RWMutex stores no owner goroutine ID. One goroutine can technically release a lock acquired by another, but that handoff must be an explicit protocol. An accidental cross-goroutine unlock breaks the business invariant.

Never copy an RWMutex after first use. `noCopy` lets `go vet -copylocks` catch many such mistakes.

### When to use it

RWMutex is a candidate when all of these hold:

- reads greatly outnumber writes;
- a read section is expensive enough for parallelism to repay coordination costs;
- readers do not modify protected state, including maps, slices, and pointed-to objects;
- temporarily stopping new reads while a writer waits is acceptable.

For a tiny single-field read, measure a regular `Mutex` or an atomic first. Frequent writes constantly close reader phases and wake queues, making RWMutex more expensive. An immutable snapshot published through `atomic.Pointer` can fit another class of read-heavy systems. Channels fit state owned by one goroutine and changed through commands.

Always benchmark the actual workload and verify correctness with `go test -race ./...` and `go vet -copylocks ./...`.

### Official references

- [sync.RWMutex source](https://go.dev/src/sync/rwmutex.go)
- [sync.RWMutex documentation](https://pkg.go.dev/sync#RWMutex)
- [The Go Memory Model](https://go.dev/ref/mem)
