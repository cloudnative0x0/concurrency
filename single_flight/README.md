# Single Flight

<p style="text-align: left">
  <a href="#русский">Русский</a> ・ <a href="#english">English</a>
</p>

---

## Русский

Single Flight — паттерн, который схлопывает несколько одновременных вызовов с одним и тем же ключом в один реальный вызов функции. Пять горутин одновременно попросили данные по ключу `"user:42"` — в базу или во внешний сервис уйдёт один запрос, а не пять, и результат этого одного запроса разойдётся всем пяти вызывающим.

Типичный случай: кэш промахнулся, и сто параллельных запросов одновременно кинулись пересчитывать одно и то же значение или бить по одному и тому же upstream-сервису. Без single flight это сто одинаковых запросов вместо одного — лишняя нагрузка на базу, лишний трафик, лишние деньги за API-вызовы.

### Как это устроено

`SingleFlight` хранит карту `calls map[string]*call`, защищённую мьютексом. Каждый `call` — это одна выполняющаяся функция: `sync.WaitGroup` для ожидания, поля `val` и `err` под результат, `panicValue` под панику и `shared` — флаг, что результат достался не только тому, кто вызов запустил.

Когда приходит `Do(key, action)`, под мьютексом проверяется, есть ли уже `call` с таким ключом:

- если нет — создаётся новый `call`, кладётся в карту, и `action` выполняется прямо сейчас в горутине вызывающего;
- если есть — вызывающий просто встаёт в очередь на `c.wg.Wait()` и ничего сам не вызывает, а ключ помечается `shared = true`.

Когда `action` завершается, `doCall` под тем же мьютексом убирает ключ из карты (если его не увели через `Forget`), зовёт `wg.Done()`, и все, кто ждал на `Wait()`, просыпаются одновременно с готовым `val`/`err`.

Если `action` паникует, паника перехватывается через `recover`, кладётся в `c.panicValue` и пробрасывается заново — сначала в той горутине, что реально исполняла `action`, а следом и во всех горутинах, которые дожидались результата через `Wait()`. Без этого паника в фоновом вызове могла бы уронить процесс так, что вызывающий код об этом даже не узнает.

`Forget(key)` — способ вручную выкинуть ключ из карты, не дожидаясь завершения текущего вызова. Полезно, если известно, что результат, который вот-вот придёт, уже устарел и следующий обратившийся должен получить свежий, а не приклеиться к старому.

### Как вызывать

```go
sf := singleflight.NewSingleFlight()

value, err, shared := sf.Do("user:42", func() (interface{}, error) {
    return fetchUserFromDB(42)
})

if err != nil {
    // обработать ошибку
}

fmt.Println(value, shared)
```

Пять параллельных запросов с одним ключом — реальный вызов `fetchUserFromDB` случится один раз:

```go
var wg sync.WaitGroup
wg.Add(5)

for i := 0; i < 5; i++ {
    go func() {
        defer wg.Done()

        value, err, shared := sf.Do("user:42", func() (interface{}, error) {
            return fetchUserFromDB(42)
        })

        fmt.Println(value, err, shared)
    }()
}

wg.Wait()
```

Принудительно сбросить результат для ключа, не дожидаясь текущего вызова:

```go
sf.Forget("user:42")
```

### Операции

| Метод | Сигнатура | Что делает |
|---|---|---|
| `Do` | `func(key string, action func() (interface{}, error)) (interface{}, error, bool)` | выполняет `action` один раз на ключ, отдаёт результат всем ожидающим, третье значение — был ли вызов разделён |
| `Forget` | `func(key string)` | убирает ключ из карты досрочно, не дожидаясь завершения текущего `action` |

### Когда использовать этот паттерн

Подходит, когда много горутин одновременно могут запросить одни и те же данные, а сам запрос дорогой: поход в базу, сетевой вызов, тяжёлое вычисление. Классика — заполнение кэша: пока значение не посчитано, все параллельные читатели ждут одного вычисления, а не запускают своё.

Не подходит, если запросы с одним ключом должны возвращать разные результаты при каждом обращении — single flight отдаёт всем один и тот же `val`/`err`, разницы между вызывающими не будет.

Не стоит применять и в местах, где вызовы и так редко пересекаются во времени — тогда весь механизм (мьютекс, карта, `WaitGroup`) — это чистые накладные расходы без выгоды.

Отдельно стоит учитывать: текущая реализация не смотрит на `context.Context`. Если один из вызывающих отменил свой контекст, он всё равно будет ждать завершения `action` наравне со всеми — прервать ожидание досрочно для одного вызывающего, не трогая остальных, эта версия не умеет.

Тесты проверяют, что `action` реально вызывается один раз на пачку конкурентных запросов, что разные ключи не блокируют друг друга, что после завершения вызова следующий `Do` с тем же ключом запускает новый `action`, что ошибка и паника из `action` доходят до всех ожидающих, и что `Forget` заставляет следующий вызов не дожидаться текущего. Гонки проверяются флагом `-race`.

---

## English

Single Flight is a pattern that collapses several concurrent calls sharing the same key into one actual function call. Five goroutines ask for data under the key `"user:42"` at the same time — the database or the external service gets one request instead of five, and the result of that one request is handed back to all five callers.

The typical trigger is a cache miss: a hundred parallel requests suddenly need the same value recomputed, or hit the same upstream service at once. Without single flight that's a hundred identical requests instead of one — extra load on the database, extra traffic, extra money spent on API calls.

### How it's built

`SingleFlight` holds a map `calls map[string]*call`, guarded by a mutex. Each `call` represents one in-flight function: a `sync.WaitGroup` to wait on, `val` and `err` fields for the result, `panicValue` for a captured panic, and `shared` — a flag marking that the result went to more than just the caller who started it.

When `Do(key, action)` comes in, it checks under the mutex whether a `call` already exists for that key:

- if not — a new `call` is created, stored in the map, and `action` runs right there in the caller's goroutine;
- if yes — the caller just joins `c.wg.Wait()` and doesn't call anything itself, and the key gets marked `shared = true`.

Once `action` finishes, `doCall` removes the key from the map under the same mutex (unless it was already taken over via `Forget`), calls `wg.Done()`, and everyone waiting on `Wait()` wakes up at once with the finished `val`/`err`.

If `action` panics, the panic is caught with `recover`, stored in `c.panicValue`, and re-raised — first in the goroutine that actually ran `action`, then in every goroutine that was waiting on `Wait()`. Without this, a panic in the background call could take down the process without the calling code ever finding out.

`Forget(key)` manually drops a key from the map without waiting for the current call to finish. Useful when the result about to arrive is already known to be stale, and the next caller should get a fresh one instead of latching onto the old call.

### How to call it

```go
sf := singleflight.NewSingleFlight()

value, err, shared := sf.Do("user:42", func() (interface{}, error) {
    return fetchUserFromDB(42)
})

if err != nil {
    // handle error
}

fmt.Println(value, shared)
```

Five parallel requests with the same key result in `fetchUserFromDB` actually running once:

```go
var wg sync.WaitGroup
wg.Add(5)

for i := 0; i < 5; i++ {
    go func() {
        defer wg.Done()

        value, err, shared := sf.Do("user:42", func() (interface{}, error) {
            return fetchUserFromDB(42)
        })

        fmt.Println(value, err, shared)
    }()
}

wg.Wait()
```

Forcing a key's result to be dropped without waiting for the current call:

```go
sf.Forget("user:42")
```

### Operations

| Method | Signature | What it does |
|---|---|---|
| `Do` | `func(key string, action func() (interface{}, error)) (interface{}, error, bool)` | runs `action` once per key, delivers the result to every waiting caller, the third value tells whether the call was shared |
| `Forget` | `func(key string)` | drops the key from the map early, without waiting for the current `action` to finish |

### When to reach for this pattern

Fits when many goroutines can request the same data at the same time and the request itself is expensive: a database round trip, a network call, a heavy computation. The classic case is cache filling — while a value is being computed, every concurrent reader waits on that one computation instead of starting its own.

Doesn't fit when calls under the same key are expected to return different results each time — single flight hands everyone the same `val`/`err`, there's no per-caller distinction.

Also not worth it in places where calls under the same key rarely overlap in time — the whole mechanism (mutex, map, `WaitGroup`) is then pure overhead with nothing to show for it.

One thing to keep in mind: the current implementation doesn't look at `context.Context`. If one of the callers cancels its own context, it still waits for `action` to finish along with everyone else — this version has no way to stop waiting early for a single caller without affecting the rest.

Tests check that `action` actually runs once for a batch of concurrent requests, that different keys don't block each other, that a `Do` call after the previous one finished starts a fresh `action`, that an error and a panic from `action` reach every waiting caller, and that `Forget` makes the next call skip waiting on the current one. Races are checked with the `-race` flag.