# Pipeline

<p style="text-align: left">
  <a href="#русский">Русский</a> ・ <a href="#english">English</a>
</p>

---

## Русский

Pipeline — цепочка стадий обработки данных, где каждая стадия сидит в своей горутине и соединена с соседями каналами. Стадия берёт значение, что-то с ним делает, отдаёт дальше. Пока вторая стадия обрабатывает первое значение, первая уже может отправлять второе — работа идёт внахлёст, а не по очереди целиком.

`generate` отдаёт `1, 2, 3, 4, 5`, `process` возводит каждое в квадрат — как только `generate` положила первую единицу в канал, `process` уже может её забрать, не дожидаясь остальных четырёх.

### Как это устроено

Каждая стадия — функция, которая создаёт канал, запускает горутину и сразу отдаёт канал вызывающему коду. Горутина внутри в этот момент может ещё вообще ничего не успеть сделать.

`generate[T any](values ...T) <-chan T` шлёт значения из `values` в `outputCh` по одному, потом закрывает канал через `defer close(outputCh)`.

`process[T any](inputCh <-chan T, action func(T) T) <-chan T` читает `inputCh` циклом `range`, применяет `action`, пишет в свой `outputCh`. Когда `range` доходит до закрытого входного канала — он сам завершается, и `process` закрывает уже свой выход.

Вот это и есть механика, которая держит всё вместе: закрытие одного канала тянет за собой закрытие следующего по цепочке. Если стадий пять подряд — закрывать нужно только первую, остальные закроются сами каскадом. Руками закрывать каждый канал не приходится.

Обе функции дженерики, так что типом можно не заморачиваться — строки, структуры, что угодно.

### Как вызывать

```go
func ExamplePipelineUsage() {
    values := []int{1, 2, 3, 4, 5}
    square := func(v int) int { return v * v }
    outputCh := generate(values...)

    for v := range process(outputCh, square) {
        fmt.Println(v)
    }
}

// Output:
// 1
// 4
// 9
// 16
// 25
```

Стадии цепляются друг за друга сколько угодно раз:

```go
squared := process(generate(1, 2, 3), func(v int) int { return v * v })
result := process(squared, func(v int) int { return v + 1 })

for v := range result {
    fmt.Println(v) // 2, 5, 10
}
```

### Операции

| Стадия | Сигнатура | Что делает |
|---|---|---|
| `generate` | `func[T any](values ...T) <-chan T` | источник данных, первая стадия |
| `process` | `func[T any](inputCh <-chan T, action func(T) T) <-chan T` | применяет функцию к каждому значению |

### Когда брать этот паттерн

Если данные должны пройти несколько шагов обработки подряд и эти шаги можно гнать параллельно, а не одним куском кода — подходит. Разобрать файл, провалидировать, записать в базу — три стадии, три горутины, никто не простаивает, пока предыдущий шаг не домучил всё целиком.

Если элементы независимы друг от друга и порядок стадий не важен — лучше взять fan-out/worker pool, там проще распараллелить по количеству воркеров, а не по количеству стадий.

Для маленького объёма данных pipeline не нужен вообще — накладные расходы на горутины и каналы перевесят выгоду, обычный цикл отработает быстрее и без единой горутины.

Ещё момент: текущие `generate` и `process` не смотрят на `context.Context`. Если никто не вычитывает канал до конца — горутина зависнет навсегда на попытке записи. Добавлять cancellation через `ctx.Done()` придётся отдельно, тут этого нет.

### Сборка и тестирование

```bash
go test -race -v ./...
```

Тесты проверяют порядок значений, работу с произвольным типом, склейку нескольких стадий и каскадное закрытие каналов. Плюс `Example`, который сверяет реальный вывод программы со строкой `// Output:`.

---

## English

Pipeline is a chain of processing stages, each sitting in its own goroutine and wired to its neighbors through channels. A stage takes a value, does something to it, hands it off. While the second stage works on the first value, the first stage can already be sending the second one — work overlaps instead of running fully sequentially.

`generate` produces `1, 2, 3, 4, 5`, `process` squares each one — as soon as `generate` puts the first value in the channel, `process` can pick it up without waiting for the other four.

### How it's built

Each stage is a function that creates a channel, starts a goroutine, and immediately hands the channel back to the caller. The goroutine itself might not have done any work yet at that point.

`generate[T any](values ...T) <-chan T` sends values from `values` into `outputCh` one at a time, then closes the channel via `defer close(outputCh)`.

`process[T any](inputCh <-chan T, action func(T) T) <-chan T` reads `inputCh` through `range`, applies `action`, writes into its own `outputCh`. Once `range` hits a closed input channel it exits on its own, and `process` closes its own output right after.

This is the mechanism holding everything together: closing one channel drags the next one closed behind it. Chain five stages and you only need to close the first one — the rest close themselves in cascade. No manual closing of every channel down the line.

Both functions are generic, so the type doesn't matter — strings, structs, whatever.

### How to call it

```go
func ExamplePipelineUsage() {
    values := []int{1, 2, 3, 4, 5}
    square := func(v int) int { return v * v }
    outputCh := generate(values...)

    for v := range process(outputCh, square) {
        fmt.Println(v)
    }
}

// Output:
// 1
// 4
// 9
// 16
// 25
```

Stages hook onto each other as many times as you want:

```go
squared := process(generate(1, 2, 3), func(v int) int { return v * v })
result := process(squared, func(v int) int { return v + 1 })

for v := range result {
    fmt.Println(v) // 2, 5, 10
}
```

### Operations

| Stage | Signature | What it does |
|---|---|---|
| `generate` | `func[T any](values ...T) <-chan T` | data source, the first stage |
| `process` | `func[T any](inputCh <-chan T, action func(T) T) <-chan T` | applies a function to each value |

### When to reach for this pattern

If data has to go through several processing steps in a row and those steps can run in parallel instead of one block of code — this fits. Parse a file, validate it, write it to a database — three stages, three goroutines, nobody sits idle waiting for the previous step to fully wrap up.

If items are independent of each other and stage order doesn't matter — fan-out/worker pool is the better call, it's simpler to scale by worker count rather than stage count.

For small amounts of data, skip pipeline entirely — the overhead of goroutines and channels outweighs the benefit, and a plain loop runs faster without spinning up a single goroutine.

One more thing: `generate` and `process` as they stand don't look at `context.Context`. If nothing reads the channel to the end, the goroutine hangs forever trying to write. Adding cancellation via `ctx.Done()` is a separate piece of work, not covered here.

### Build and test

```bash
go test -race -v ./...
```

Tests cover value ordering, working with an arbitrary type, chaining several stages, and cascading channel closure. Plus an `Example` that checks the program's actual output against the `// Output:` line.