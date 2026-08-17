# Semaphore

<p style="text-align: left">
  <a href="#русский">Русский</a> ・ <a href="#english">English</a>
</p>

---

## Русский

Semaphore — примитив синхронизации, который ограничивает число горутин, работающих с ресурсом одновременно. Внутри — буферизированный канал: место в буфере занято, значит слот выдан, место освободилось — слот вернулся. Пять горутин просят доступ, а слотов три — две подождут, пока кто-то из первых трёх не освободит место.

`NewSemaphore(3)` даёт семафор на три параллельных доступа. Четвёртая горутина, вызвавшая `Acquire`, заблокируется, пока одна из первых трёх не вызовет `release()`.

### Как это устроено

`Acquire(ctx)` сначала проверяет, не закрыт ли семафор — под `RLock`, чтобы не блокировать параллельные вызовы друг друга. Если закрыт — сразу `ErrSemaphoreClosed`. Если нет — `select` между отправкой в `tokens` и `ctx.Done()`. Кто сработает первым, то и определит исход: либо слот занят и возвращается функция `release`, либо контекст истёк и возвращается его ошибка.

`release` оборачивает чтение из канала в `sync.Once`. Без этого повторный вызов `release()` (например, случайно вызванный дважды в разных местах кода) вычитывал бы из канала лишний раз и тем самым отдавал бы в оборот слот, который на самом деле никто не занимал — семафор начал бы пропускать больше горутин, чем задумано.

`TryAcquire()` — та же логика, но без ожидания: `select` с `default`. Если слот свободен — забирает сразу, если нет — сразу возвращает `false`, без блокировки.

`Close()` не трогает уже занятые слоты и не разрывает уже выданные `release`. Он только запрещает новые вызовы `Acquire` — горутины, которые уже держат слот, спокойно доработают и освободят его как обычно.

`noCopy` встроен в структуру, чтобы `go vet -copylocks` ловил случайное копирование `Semaphore` по значению — семафор хранит состояние в канале и мьютексе, копия расходится с оригиналом и перестаёт с ним синхронизироваться.

### Как вызывать

```go
sem := semaphore.NewSemaphore(3)

ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()

release, err := sem.Acquire(ctx)
if err != nil {
    return err
}
defer release()

// работа с ограниченным ресурсом
```

Неблокирующий вариант:

```go
release, ok := sem.TryAcquire()
if !ok {
    // слотов нет, работаем без ожидания
    return
}
defer release()
```

Закрытие семафора для новых обращений:

```go
sem.Close()

_, err := sem.Acquire(context.Background())
// err == semaphore.ErrSemaphoreClosed
```

### Операции

| Метод | Сигнатура | Что делает |
|---|---|---|
| `NewSemaphore` | `func(n int) *Semaphore` | создаёт семафор ёмкостью `n`, паникует при `n <= 0` |
| `Acquire` | `func(ctx context.Context) (release func(), err error)` | ждёт свободный слот или отмену контекста |
| `TryAcquire` | `func() (release func(), ok bool)` | занимает слот без ожидания, если он свободен |
| `Close` | `func()` | запрещает новые `Acquire`, уже выданные `release` продолжают работать |

### Когда брать этот примитив

Если нужно ограничить число горутин, одновременно работающих с ресурсом — открытых соединений с базой, обращений к внешнему API с рейт-лимитом, файлов, открытых на запись — семафор подходит напрямую. Ёмкость задаётся один раз и держит потолок параллелизма вне зависимости от того, сколько горутин вообще запущено в программе.

Если ограничение сводится к взаимоисключающему доступу одного владельца — то есть слот либо занят, либо свободен, без счётчика — обычный `sync.Mutex` проще и быстрее, семафор ёмкостью 1 не даёт тут ничего, кроме лишнего слоя абстракции.

Если задача — не ограничить параллелизм, а дождаться завершения фиксированного числа горутин, — нужен `sync.WaitGroup`, а не семафор, у них разное назначение при похожем виде использования.

Для организации самого пула воркеров с очередью задач семафор — не замена worker pool, а его внутренний инструмент: пул задаёт форму обработки (горутины, канал задач), семафор внутри него может держать потолок на конкретном ограниченном ресурсе, если пул работает не с одним, а сразу с несколькими такими ресурсами.

### Известное ограничение

В `noCopy` метод называется `UnLock`, а не `Unlock`. `go vet -copylocks` ищет ровно интерфейс `sync.Locker` — `Lock()` и `Unlock()` с точным написанием — и с такой опечаткой защита от копирования `Semaphore` по значению не сработает: `go vet` пройдёт молча, даже если структуру скопировали. Это не влияет на работу самого семафора, но перед использованием в прод-коде опечатку стоит поправить и добавить `go vet -copylocks ./...` в проверки.

Тесты должны покрывать: превышение ёмкости под конкурентной нагрузкой, поведение при отмене контекста, повторный вызов `release()`, `Close()` во время параллельных вызовов `Acquire`.

---

## English

Semaphore is a synchronization primitive that limits how many goroutines can work with a resource at the same time. Underneath is a buffered channel: an occupied slot in the buffer means a token is held, a freed slot means it's back. Five goroutines ask for access, only three slots exist — two wait until one of the first three lets go.

`NewSemaphore(3)` gives a semaphore with three concurrent slots. A fourth goroutine calling `Acquire` blocks until one of the first three calls `release()`.

### How it's built

`Acquire(ctx)` first checks whether the semaphore is closed, under `RLock` so concurrent calls don't block each other. If it's closed, it returns `ErrSemaphoreClosed` right away. If not, it runs a `select` between sending into `tokens` and `ctx.Done()`. Whichever fires first decides the outcome: either a slot is taken and a `release` function comes back, or the context expired and its error comes back instead.

`release` wraps the channel read in `sync.Once`. Without that, calling `release()` twice — say, accidentally called from two different places — would read from the channel an extra time and hand back a slot nobody actually held, letting the semaphore admit more goroutines than it was built for.

`TryAcquire()` follows the same logic but without waiting: a `select` with a `default` case. If a slot is free it's taken immediately, if not it returns `false` right away, no blocking involved.

`Close()` doesn't touch slots already held and doesn't invalidate `release` functions already handed out. It only blocks new `Acquire` calls — goroutines already holding a slot finish their work and release it as usual.

`noCopy` is embedded so `go vet -copylocks` catches an accidental copy of `Semaphore` by value — the semaphore keeps its state in a channel and a mutex, and a copy drifts out of sync with the original.

### How to call it

```go
sem := semaphore.NewSemaphore(3)

ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()

release, err := sem.Acquire(ctx)
if err != nil {
    return err
}
defer release()

// work with the limited resource
```

Non-blocking version:

```go
release, ok := sem.TryAcquire()
if !ok {
    // no slots available, proceed without waiting
    return
}
defer release()
```

Closing the semaphore to new callers:

```go
sem.Close()

_, err := sem.Acquire(context.Background())
// err == semaphore.ErrSemaphoreClosed
```

### Operations

| Method | Signature | What it does |
|---|---|---|
| `NewSemaphore` | `func(n int) *Semaphore` | creates a semaphore with capacity `n`, panics if `n <= 0` |
| `Acquire` | `func(ctx context.Context) (release func(), err error)` | waits for a free slot or for the context to be cancelled |
| `TryAcquire` | `func() (release func(), ok bool)` | takes a slot without waiting if one is free |
| `Close` | `func()` | blocks new `Acquire` calls, already-issued `release` functions keep working |

### When to reach for this primitive

If you need to cap how many goroutines work with a resource at once — open database connections, calls to a rate-limited external API, files open for writing — a semaphore fits directly. Capacity is set once and holds the concurrency ceiling regardless of how many goroutines are running in the program overall.

If the limit boils down to exclusive access by a single owner — a slot is either taken or free, no counting involved — a plain `sync.Mutex` is simpler and faster; a semaphore with capacity 1 adds nothing here beyond an extra layer of abstraction.

If the task is waiting for a fixed number of goroutines to finish rather than limiting concurrency, that's `sync.WaitGroup`, not a semaphore — they look similar in usage but serve different purposes.

For building a worker pool itself, a semaphore isn't a replacement for it — it's a tool the pool can use internally. The pool shapes the processing (goroutines, a task channel), and a semaphore inside it can cap access to one specific limited resource if the pool works with more than one such resource at a time.

### Known limitation

In `noCopy`, the method is named `UnLock`, not `Unlock`. `go vet -copylocks` looks for the exact `sync.Locker` interface — `Lock()` and `Unlock()`, spelled precisely — and with this typo the copy protection on `Semaphore` doesn't trigger: `go vet` passes silently even when the struct gets copied by value. It doesn't affect how the semaphore behaves, but before using this in production code the typo should be fixed, along with adding `go vet -copylocks ./...` to the checks.

Tests should cover: exceeding capacity under concurrent load, behavior on context cancellation, calling `release()` more than once, and `Close()` firing while `Acquire` calls are in flight.