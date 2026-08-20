# Promise

<p style="text-align: left">
  <a href="#русский">Русский</a> ・ <a href="#english">English</a>
</p>

---

## Русский

Promise (он же Future) — обёртка над результатом асинхронной операции, который ещё не готов, но станет готов позже. Функция запускается в отдельной горутине сразу при создании `Promise`, а вызывающий код получает не значение, а объект, у которого можно либо синхронно дождаться результата, либо подписаться на него колбэками — сколько угодно раз и когда угодно, хоть до завершения работы, хоть после.

`NewPromise` не блокирует вызывающую горутину — она сразу получает `*Promise[T]` и продолжает работать дальше, пока результат вычисляется параллельно.

### Как это устроено

Внутри `Promise` нет канала с данными — есть закрывающийся канал `done chan struct{}` и поле `result`, куда горутина один раз пишет значение перед тем, как этот канал закрыть.

```go
type Promise[T any] struct {
    done   chan struct{}
    result result[T]
}
```

Разница с обычным подходом "передать результат через канал" принципиальна: закрытый канал можно читать из скольких угодно горутин сколько угодно раз, и каждое чтение сразу разблокируется. Обычный канал с данными отдаёт значение только одному читателю — второй подписчик просто зависнет. Поэтому здесь `done` закрывается один раз через `defer close(p.done)`, а само значение лежит рядом в `p.result`, откуда его читают все, кто дождался закрытия канала.

`NewPromise[T any](ctx context.Context, asyncFn func(context.Context) (T, error)) *Promise[T]` запускает `asyncFn` в горутине, оборачивает вызов в `recover()` — если функция запаникует, паника превратится в обычный `error`, а не уронит процесс и не оставит подписчиков висеть без ответа навсегда.

`Then(successFn func(T), errorFn func(error))` подписывается на результат в отдельной горутине: дожидается закрытия `done` и вызывает нужный колбэк. Можно вызвать `Then` хоть до того, как асинхронная функция закончила работу, хоть спустя час после этого — оба раза колбэк получит значение.

`Await() (T, error)` — блокирующий вариант получить результат синхронно, без колбэков.

`AwaitContext(ctx context.Context) (T, error)` — то же самое, но ожидание можно прервать по контексту: если `ctx` завершится раньше, чем `asyncFn`, метод вернёт `ctx.Err()`, не дожидаясь самой операции.

### Как вызывать

```go
func ExamplePromiseUsage() {
    asyncJob := func(ctx context.Context) (string, error) {
        time.Sleep(100 * time.Millisecond)
		
        return "done", nil
    }

    p := NewPromise(context.Background(), asyncJob)
    
    val, err := p.Await()
    fmt.Println(val, err)
}

// Output:
// done <nil>
```

Подписка колбэками вместо блокирующего ожидания:

```go
p := NewPromise(context.Background(), asyncJob)

p.Then(
    func(value string) { fmt.Println("success:", value) },
    func(err error) { fmt.Println("error:", err) },
)
```

Один и тот же `Promise` можно раздать нескольким подписчикам — каждый получит один и тот же результат независимо от остальных:

```go
p := NewPromise(context.Background(), asyncJob)

    p.Then(func(v string) { fmt.Println("subscriber 1:", v) }, nil)
    p.Then(func(v string) { fmt.Println("subscriber 2:", v) }, nil)
```

Ожидание с таймаутом:

```go
ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
defer cancel()

val, err := p.AwaitContext(ctx)
if errors.Is(err, context.DeadlineExceeded) {
    fmt.Println("не успели")
}
```

### Операции

| Метод | Сигнатура | Что делает |
|---|---|---|
| `NewPromise` | `func[T any](ctx context.Context, asyncFn func(context.Context) (T, error)) *Promise[T]` | запускает асинхронную операцию, возвращает промис сразу |
| `Then` | `func (p *Promise[T]) Then(successFn func(T), errorFn func(error))` | подписывается на результат колбэками, не блокирует вызывающую горутину |
| `Await` | `func (p *Promise[T]) Await() (T, error)` | блокирует текущую горутину до готовности результата |
| `AwaitContext` | `func (p *Promise[T]) AwaitContext(ctx context.Context) (T, error)` | то же самое, но ожидание прерывается по контексту |

### Когда брать этот паттерн

Если запускается одна асинхронная операция, результат которой нужен где-то дальше по коду — не сразу, не в этом месте вызова, а спустя какое-то время или в другом месте программы — Promise подходит. Сходить в внешний сервис за данными, пока параллельно готовится остальная часть ответа, а потом свести всё воедино — типичный случай.

Если нужно запустить много независимых задач и дождаться всех — `errgroup` или обычный `sync.WaitGroup` проще и без лишней обёртки. Promise не заменяет worker pool и не масштабируется по количеству воркеров — он про одну операцию с одним результатом, который может понадобиться в нескольких местах.

Если результат нужен ровно один раз и сразу там же, где запускается операция — оборачивать в Promise незачем, обычный синхронный вызов или горутина с одним каналом отработают без лишнего кода.

Момент, который стоит держать в голове: `AwaitContext` прерывает только ожидание, а не саму `asyncFn` — если функция игнорирует переданный ей `ctx`, она продолжит работать в фоне после того, как `AwaitContext` уже вернул `context.DeadlineExceeded`. Чтобы операция реально останавливалась, `asyncFn` должна сама проверять `ctx.Done()`.

Тесты проверяют доставку результата одному и нескольким подписчикам, синхронное и контекстное ожидание, восстановление после паники в `asyncFn` и отсутствие утечек горутин при повторной подписке и при полном отсутствии подписчиков.

---

## English

Promise (also known as Future) is a wrapper around the result of an asynchronous operation that isn't ready yet but will be later. The function starts running in its own goroutine the moment `Promise` is created, and the calling code gets back not a value but an object it can either wait on synchronously or subscribe to with callbacks — as many times as it wants, whenever it wants, whether the operation has already finished or not.

`NewPromise` doesn't block the calling goroutine — it hands back a `*Promise[T]` right away and keeps running while the result is computed in parallel.

### How it's built

There's no data channel inside `Promise` — there's a closing channel `done chan struct{}` and a `result` field the goroutine writes to exactly once before closing that channel.

```go
type Promise[T any] struct {
    done   chan struct{}
    result result[T]
}
```

The difference from the usual "pass the result through a channel" approach matters here: a closed channel can be read from as many goroutines as you like, as many times as you like, and every read unblocks immediately. A regular data channel hands its value to a single reader — a second subscriber would just hang. That's why `done` gets closed exactly once via `defer close(p.done)`, while the value itself sits next to it in `p.result`, read by everyone who's waited for the channel to close.

`NewPromise[T any](ctx context.Context, asyncFn func(context.Context) (T, error)) *Promise[T]` runs `asyncFn` in a goroutine, wrapping the call in `recover()` — if the function panics, the panic turns into a regular `error` instead of crashing the process or leaving subscribers hanging forever with no answer.

`Then(successFn func(T), errorFn func(error))` subscribes to the result in a separate goroutine: it waits for `done` to close and calls the right callback. `Then` can be called before the async function has finished, or an hour after it has — either way the callback gets the value.

`Await() (T, error)` is the blocking way to get the result synchronously, without callbacks.

`AwaitContext(ctx context.Context) (T, error)` does the same thing, but the wait can be cut short by context: if `ctx` finishes before `asyncFn` does, the method returns `ctx.Err()` without waiting for the operation itself.

### How to call it

```go
func ExamplePromiseUsage() {asyncJob := func(ctx context.Context) (string, error) {
        time.Sleep(100 * time.Millisecond)
        return "done", nil
    }

    p := NewPromise(context.Background(), asyncJob)
    
    val, err := p.Await()
    fmt.Println(val, err)
}

// Output:
// done <nil>
```

Subscribing with callbacks instead of blocking:

```go
p := NewPromise(context.Background(), asyncJob)

p.Then(
    func(value string) { fmt.Println("success:", value) },
    func(err error) { fmt.Println("error:", err) },
)
```

The same `Promise` can be handed to several subscribers — each gets the same result independently of the others:

```go
p := NewPromise(context.Background(), asyncJob)

p.Then(func(v string) { fmt.Println("subscriber 1:", v) }, nil)
p.Then(func(v string) { fmt.Println("subscriber 2:", v) }, nil)
```

Waiting with a timeout:

```go
ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
defer cancel()

val, err := p.AwaitContext(ctx)
if errors.Is(err, context.DeadlineExceeded) {
    fmt.Println("didn't make it")
}
```

### Operations

| Method | Signature | What it does |
|---|---|---|
| `NewPromise` | `func[T any](ctx context.Context, asyncFn func(context.Context) (T, error)) *Promise[T]` | starts an async operation, returns the promise right away |
| `Then` | `func (p *Promise[T]) Then(successFn func(T), errorFn func(error))` | subscribes to the result with callbacks, doesn't block the calling goroutine |
| `Await` | `func (p *Promise[T]) Await() (T, error)` | blocks the current goroutine until the result is ready |
| `AwaitContext` | `func (p *Promise[T]) AwaitContext(ctx context.Context) (T, error)` | same thing, but the wait can be cancelled via context |

### When to reach for this pattern

If a single asynchronous operation is started and its result is needed further down the code — not right away, not at the call site, but after some time or somewhere else in the program — Promise fits. Kick off a call to an external service while the rest of a response gets prepared in parallel, then bring it all together later — that's the typical case.

If many independent tasks need to run and all be waited on — `errgroup` or a plain `sync.WaitGroup` is simpler and doesn't need the extra wrapper. Promise doesn't replace a worker pool and doesn't scale by worker count — it's about one operation with one result that might be needed in more than one place.

If the result is only needed once and right where the operation is started — wrapping it in a Promise buys nothing; a plain synchronous call or a goroutine with a single channel gets the job done with less code.

One thing worth keeping in mind: `AwaitContext` only cuts short the waiting, not `asyncFn` itself — if the function ignores the `ctx` passed to it, it keeps running in the background after `AwaitContext` has already returned `context.DeadlineExceeded`. For the operation to actually stop, `asyncFn` has to check `ctx.Done()` on its own.

Tests cover delivering the result to a single subscriber and to several at once, synchronous and context-bound waiting, recovering from a panic inside `asyncFn`, and the absence of goroutine leaks both with repeated subscriptions and with no subscribers at all.