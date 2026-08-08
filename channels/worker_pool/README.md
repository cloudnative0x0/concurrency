# Worker Pool

<p style="text-align: left">
  <a href="#русский">Русский</a> ・ <a href="#english">English</a>
</p>

---

## Русский

Worker pool — паттерн, в котором несколько горутин читают задачи из одного общего канала и обрабатывают их параллельно. В отличие от fan-out, где каждый воркер получает свой отдельный канал и заранее расписанную долю работы, здесь канал один на всех: как только воркер освобождается, он сам забирает из него следующее доступное значение. Распределение нагрузки получается динамическим — оно подстраивается под то, кто из воркеров реально свободен, а не следует жёсткому расписанию.

В примере ниже `parse` пропускает поток транзакций через себя без изменений, а `send` запускает `n` воркеров, каждый из которых читает из общего входного канала и пишет результат в общий выходной. Все `n` воркеров конкурируют за один и тот же источник задач.

### Как это устроено

`parse[T any](inputCh <-chan T) <-chan T` создаёt выходной канал и запускает одну горутину, которая вычитывает `inputCh` через `range` и пересылает каждое значение в `outputCh`. Когда `inputCh` закрывается, `range` завершается, `defer close(outputCh)` закрывает выход следом. Сама по себе эта функция параллелизма не добавляет — она просто оборачивает канал в отдельную стадию пайплайна, через которую удобно пропускать промежуточную обработку (парсинг, валидацию и так далее).

`send[T any](inputCh <-chan T, n int) <-chan T` — собственно worker pool. Функция создаёт `sync.WaitGroup` на `n` воркеров и запускает `n` горутин, каждая из которых в цикле `for val := range inputCh` читает из **одного и того же** `inputCh` и пишет результат в общий `outputCh`. Здесь и заключается ключевое отличие от fan-out: канал `inputCh` не делится на части заранее — все воркеры читают из него напрямую, и то, какое значение достанется какому воркеру, решает рантайм в момент, когда воркер готов принять следующую задачу.

Отдельная горутина с `wg.Wait()` дожидается, пока все `n` воркеров завершат свою работу (то есть пока `inputCh` не закроется и каждый воркер не выйдет из `range`), и только после этого закрывает `outputCh`. Без этого шага чтение из `outputCh` в вызывающем коде никогда бы не завершилось — канал остался бы открытым навсегда, даже когда писать в него больше некому.

### Как вызывать

```go
type Transaction struct {
	ID     int
	From   string
	To     string
	Amount float64
}

func ExampleWorkerPoolUsage() {
	mempool := make(chan Transaction)
	go func() {
		defer close(mempool)
		txs := []Transaction{
			{1, "Alice", "Bob", 0.015},
			{2, "Bob", "Carol", 0.2},
			{3, "Carol", "Dave", 1.5},
			{4, "Dave", "Eve", 0.001},
			{5, "Eve", "Alice", 3.14},
		}
		for _, tx := range txs {
			mempool <- tx
		}
	}()

	parsed := parse(mempool)

	broadcasted := send(parsed, 3)

	for tx := range broadcasted {
		fmt.Printf("broadcasted tx #%d: %s -> %s (%.4f BTC)\n", tx.ID, tx.From, tx.To, tx.Amount)
	}
}
```

Что здесь происходит по шагам:

1. Горутина имитирует мемпул: кладёт пять транзакций в канал `mempool` одну за другой и закрывает канал через `defer`, когда все пять отправлены.
2. `parse(mempool)` оборачивает `mempool` в промежуточную стадию — здесь можно было бы добавить проверку подписи или десериализацию, но в текущем виде значения просто проходят насквозь.
3. `send(parsed, 3)` запускает три воркера. Все три читают из одного и того же канала `parsed` — как только воркер освобождается, он забирает следующую готовую транзакцию, а не ждёт своей заранее выделенной доли, как было бы при fan-out. Если одна из транзакций обрабатывалась бы дольше остальных, два других воркера продолжали бы разбирать очередь, не простаивая в ожидании.
4. Основной цикл `for tx := range broadcasted` вычитывает результат по мере готовности и печатает его. Цикл завершается сам, когда `send` закрывает `outputCh` после того, как все воркеры отработали.

Порядок, в котором транзакции появятся в `broadcasted`, не гарантирован — три воркера работают независимо, и та транзакция, что досталась воркеру раньше и обработалась быстрее, окажется на выходе первой, вне зависимости от исходного порядка в `mempool`.

### Операции

| Функция | Сигнатура | Что делает |
|---|---|---|
| `parse` | `func[T any](inputCh <-chan T) <-chan T` | промежуточная стадия пайплайна, пропускает значения через себя |
| `send` | `func[T any](inputCh <-chan T, n int) <-chan T` | запускает n воркеров, читающих из общего канала |

### Когда брать этот паттерн

Worker pool подходит, когда задач много, они однотипны и разной длины по времени обработки — сетевые запросы к разным узлам с разной задержкой, обработка файлов разного размера, любая работа, где заранее неизвестно, сколько времени займёт конкретный элемент. Поскольку все воркеры тянут из одного канала, простаивающий воркер тут же подхватывает следующую задачу вместо того, чтобы ждать своей очереди — в этом главное преимущество перед fan-out со статичным round-robin распределением.

Из этого же устройства вытекают и ограничения. Общий канал — это единая точка синхронизации: если запись в `inputCh` или чтение из `outputCh` идёт медленно, все `n` воркеров будут упираться в один и тот же канал, и рост числа воркеров перестанет давать линейный прирост производительности. Порядок обработки не сохраняется, и если он важен, поверх worker pool придётся добавлять отдельную сортировку или нумерацию результатов. Число `n` тоже приходится подбирать вручную — слишком много воркеров при дешёвой обработке даст накладные расходы на переключение горутин без реальной выгоды, слишком мало — не раскроет доступный параллелизм.

Для небольшого числа задач или дешёвой обработки на одну задачу пул воркеров не нужен — стоимость запуска горутин и синхронизации через канал и `WaitGroup` перевесит выигрыш от параллелизма, и обычный последовательный цикл отработает быстрее.

### Сборка и тестирование

```bash
go test -race -v ./...
```

Флаг `-race` здесь обязателен: несколько воркеров пишут в общий `outputCh` конкурентно, и любая ошибка синхронизации проявится именно под race detector'ом.

---

## English

Worker pool is a pattern where several goroutines read tasks from one shared channel and process them in parallel. Unlike fan-out, where each worker gets its own dedicated channel with a pre-scheduled share of the work, here there's a single channel for everyone: as soon as a worker frees up, it pulls the next available value from it itself. Load balancing ends up dynamic — it adapts to which worker is actually free, rather than following a fixed schedule.

In the example below, `parse` passes a stream of transactions through unchanged, and `send` starts `n` workers, each reading from the shared input channel and writing its result to the shared output channel. All `n` workers compete for the same source of tasks.

### How it's built

`parse[T any](inputCh <-chan T) <-chan T` creates an output channel and starts a single goroutine that reads `inputCh` through `range` and forwards every value into `outputCh`. Once `inputCh` closes, `range` exits, and `defer close(outputCh)` closes the output right after. On its own, this function adds no parallelism — it just wraps a channel into a separate pipeline stage, a convenient place to insert intermediate processing like parsing or validation.

`send[T any](inputCh <-chan T, n int) <-chan T` is the actual worker pool. The function creates a `sync.WaitGroup` sized for `n` workers and starts `n` goroutines, each reading from the **same** `inputCh` in a `for val := range inputCh` loop and writing its result into the shared `outputCh`. This is the key difference from fan-out: `inputCh` isn't split into pieces ahead of time — every worker reads from it directly, and which value goes to which worker is decided by the runtime at the moment a worker is ready for its next task.

A separate goroutine with `wg.Wait()` waits for all `n` workers to finish (that is, until `inputCh` closes and every worker exits its `range` loop), and only then closes `outputCh`. Without this step, reading from `outputCh` in the caller would never finish — the channel would stay open forever, even once nothing is left to write into it.

### How to call it

```go
type Transaction struct {
	ID     int
	From   string
	To     string
	Amount float64
}

func ExampleWorkerPoolUsage() {
	mempool := make(chan Transaction)
	go func() {
		defer close(mempool)
		txs := []Transaction{
			{1, "Alice", "Bob", 0.015},
			{2, "Bob", "Carol", 0.2},
			{3, "Carol", "Dave", 1.5},
			{4, "Dave", "Eve", 0.001},
			{5, "Eve", "Alice", 3.14},
		}
		for _, tx := range txs {
			mempool <- tx
		}
	}()

	parsed := parse(mempool)

	broadcasted := send(parsed, 3)

	for tx := range broadcasted {
		fmt.Printf("broadcasted tx #%d: %s -> %s (%.4f BTC)\n", tx.ID, tx.From, tx.To, tx.Amount)
	}
}
```

Step by step, this is what happens:

1. A goroutine simulates a mempool: it puts five transactions into the `mempool` channel one after another and closes the channel via `defer` once all five are sent.
2. `parse(mempool)` wraps `mempool` into an intermediate stage — this is where signature checking or deserialization could be added, but in its current form values just pass through untouched.
3. `send(parsed, 3)` starts three workers. All three read from the same `parsed` channel — as soon as a worker frees up, it grabs the next ready transaction instead of waiting for a pre-assigned share, the way it would with fan-out. If one transaction took longer to process than the others, the remaining two workers would keep pulling from the queue instead of sitting idle.
4. The main loop `for tx := range broadcasted` reads results as they become ready and prints them. The loop exits on its own once `send` closes `outputCh` after every worker has finished.

The order in which transactions show up in `broadcasted` isn't guaranteed — the three workers run independently, and whichever transaction a worker picked up earlier and finished processing first ends up first on the output, regardless of its original order in `mempool`.

### Operations

| Function | Signature | What it does |
|---|---|---|
| `parse` | `func[T any](inputCh <-chan T) <-chan T` | intermediate pipeline stage, passes values through |
| `send` | `func[T any](inputCh <-chan T, n int) <-chan T` | starts n workers reading from a shared channel |

### When to reach for this pattern

Worker pool fits when there are many similar tasks with uneven processing time — network requests to different nodes with varying latency, processing files of different sizes, any work where you don't know upfront how long a given element will take. Since every worker pulls from the same channel, an idle worker immediately picks up the next task instead of waiting its turn — that's the main advantage over fan-out's static round-robin distribution.

The same design brings its own limits. A shared channel is a single synchronization point: if writing to `inputCh` or reading from `outputCh` is slow, all `n` workers end up bottlenecked on that same channel, and adding more workers stops giving a linear performance gain. Processing order isn't preserved, and if order matters, a separate sorting or numbering step has to be added on top of the pool. The value of `n` also has to be tuned by hand — too many workers for cheap processing adds goroutine-switching overhead without real benefit, too few leaves available parallelism on the table.

For a small number of tasks or cheap per-task processing, a worker pool isn't worth it — the cost of spinning up goroutines and synchronizing through a channel and a `WaitGroup` outweighs the gain from parallelism, and a plain sequential loop runs faster.

### Build and test

```bash
go test -race -v ./...
```

The `-race` flag is mandatory here: several workers write to the shared `outputCh` concurrently, and any synchronization bug will show up under the race detector.