# Fan-Out

<p style="text-align: left">
  <a href="#русский">Русский</a> ・ <a href="#english">English</a>
</p>

---

## Русский

Fan-out — паттерн, обратный fan-in: один входной канал разбивается на несколько выходных, и каждое значение из входа уходит ровно в один из них по кругу. Источник данных при этом один и работает сам по себе, а вот обработчиков — сколько нужно, и все они читают свою часть потока параллельно, не мешая друг другу.

В примере ниже одна горутина кладёт в канал `txs` девять транзакций подряд. `splitCh` разбирает их на три канала — `workerChs[0]`, `workerChs[1]`, `workerChs[2]` — и три воркера параллельно рассылают свою часть транзакций в сеть через `broadcastTx`, вместо того чтобы делать это одной горутиной строго по очереди.

### Как это устроено

`splitCh[T any](inputCh <-chan T, n int) []<-chan T` сперва создаёт `n` каналов и кладёт их в `outputChs`. Дальше запускается одна горутина, которая читает `inputCh` через `range` и раскладывает значения по каналам из `outputChs` по кругу — эту работу делает строчка `idx = (idx + 1) % n`: `idx` растёт на единицу после каждой записи, а `% n` возвращает его к нулю, как только он доходит до последнего индекса. Три канала — значения идут `0, 1, 2, 0, 1, 2, ...`, десять каналов — `0, 1, ..., 9, 0, 1, ...`.

Когда `inputCh` закрывается, `range` внутри горутины завершается сам, и следом идёт цикл `for _, ch := range outputChs { close(ch) }` — закрываются все выходные каналы разом. Отсюда вытекает важная деталь: пока `inputCh` не закрыт, ни один из `outputChs` не закроется, сколько бы значений через них ни прошло.

Функция возвращает `[]<-chan T`, а не `[]chan T` — вызывающий код может только читать из этих каналов, писать в них или закрывать их напрямую он не может. Это на уровне типов исключает ситуацию, когда кто-то снаружи случайно закроет канал, за который отвечает `splitCh`.

### Как вызывать

```go
package main

import (
	"fmt"
	"sync"
	"time"
)

type transaction struct {
	txID   string
	amount float64
}

// broadcastTx имитирует отправку транзакции в сеть — в реальности
// здесь был бы RPC-вызов к ноде вроде sendrawtransaction.
func broadcastTx(workerID int, tx transaction) {
	time.Sleep(50 * time.Millisecond) // имитация сетевой задержки
	fmt.Printf("worker %d: транзакция %s на %.4f BTC отправлена в сеть\n", workerID, tx.txID, tx.amount)
}

func main() {
	txs := make(chan transaction)
	go func() {
		defer close(txs)
		for i := 1; i <= 9; i++ {
			txs <- transaction{
				txID:   fmt.Sprintf("tx_%03d", i),
				amount: float64(i) * 0.001,
			}
		}
	}()

	workerChs := splitCh(txs, 3)

	var wg sync.WaitGroup
	wg.Add(len(workerChs))
	for workerID, ch := range workerChs {
		go func(workerID int, ch <-chan transaction) {
			defer wg.Done()
			for tx := range ch {
				broadcastTx(workerID, tx)
			}
		}(workerID, ch)
	}
	wg.Wait()
}
```

Что здесь происходит по шагам:

1. Горутина генерирует девять транзакций `tx_001 .. tx_009` и пишет их в `txs`, закрывая канал в конце через `defer`.
2. `splitCh(txs, 3)` запускает внутри себя горутину-распределитель и сразу возвращает три канала — воркер 0 получит транзакции `tx_001, tx_004, tx_007`, воркер 1 — `tx_002, tx_005, tx_008`, воркер 2 — `tx_003, tx_006, tx_009`.
3. На каждый из трёх каналов запускается свой воркер. Он вычитывает свою часть транзакций циклом `range` и для каждой вызывает `broadcastTx` — функция имитирует сетевую задержку через `time.Sleep(50 * time.Millisecond)`, как будто реально стучится в ноду.
4. `sync.WaitGroup` держит `main` живым, пока все три воркера не разошлют свою часть транзакций и не выйдут из `range` после закрытия своих каналов.

Ключевая выгода видна на цифрах: девять транзакций по 50 миллисекунд каждая — это 450 миллисекунд, если слать их одной горутиной подряд. С тремя воркерами это укладывается примерно в 150 миллисекунд, потому что задержки трёх независимых отправок идут параллельно, а не одна за другой.

### Операции

| Функция | Сигнатура | Что делает |
|---|---|---|
| `splitCh` | `func[T any](inputCh <-chan T, n int) []<-chan T` | раскладывает один канал на N по кругу |

### Когда брать этот паттерн

Подходит, когда есть один источник данных и по-настоящему независимая друг от друга работа над каждым элементом — рассылка транзакций по разным пирам, обработка входящих запросов, любая задача, где элемент можно обработать, не зная ничего про соседние. Чем дороже обработка одного элемента (сетевой вызов, тяжёлые вычисления, запись в БД), тем заметнее выигрыш от распараллеливания через fan-out.

Round-robin в `splitCh` не смотрит на то, кто из воркеров уже освободился, а кто ещё занят — раздача идёт строго по кругу независимо от реальной загрузки. Если воркеры или элементы сильно отличаются по времени обработки, кто-то из воркеров будет регулярно простаивать в ожидании следующего значения, пока другой ещё разгребает очередь. Для равномерной загрузки при неравномерной работе больше подходит схема с общим каналом, откуда воркеры сами разбирают задачи по готовности, а не получают их по расписанию.

Для маленького количества элементов или дешёвой обработки паттерн не нужен — накладные расходы на горутины и синхронизацию перевесят выгоду от параллелизма, обычный цикл без единого воркера справится быстрее.

Fan-out часто идёт в паре с fan-in: разбили работу на воркеров, каждый пишет в свой канал, а потом свели всё обратно в один поток, если нужен единый результат, а не просто побочный эффект вроде рассылки в сеть, как в примере выше.

### Сборка и тестирование

```bash
go test -race -v ./...
```

Флаг `-race` важен здесь так же, как и в fan-in: несколько воркеров работают параллельно, и любая ошибка синхронизации при распределении по каналам проявится именно под race detector'ом.

---

## English

Fan-out is the inverse of fan-in: one input channel gets split into several output channels, and every value from the input goes to exactly one of them, round-robin. There's a single data source running on its own, and as many handlers as needed, each reading its own slice of the stream in parallel without stepping on each other.

In the example below, one goroutine puts nine transactions into the `txs` channel one after another. `splitCh` splits them across three channels — `workerChs[0]`, `workerChs[1]`, `workerChs[2]` — and three workers broadcast their share of transactions to the network through `broadcastTx` in parallel, instead of a single goroutine doing it strictly one at a time.

### How it's built

`splitCh[T any](inputCh <-chan T, n int) []<-chan T` first creates `n` channels and stores them in `outputChs`. Then a single goroutine reads `inputCh` through `range` and distributes values across `outputChs` round-robin — that's what the line `idx = (idx + 1) % n` does: `idx` increments by one after every write, and `% n` wraps it back to zero once it hits the last index. Three channels — values go `0, 1, 2, 0, 1, 2, ...`, ten channels — `0, 1, ..., 9, 0, 1, ...`.

Once `inputCh` closes, `range` inside the goroutine exits on its own, followed by `for _, ch := range outputChs { close(ch) }` — every output channel closes at once. This has a direct consequence: none of the `outputChs` close while `inputCh` is still open, no matter how many values already went through.

The function returns `[]<-chan T`, not `[]chan T` — the caller can only read from these channels, not write to them or close them directly. That rules out, at the type level, the case where someone outside accidentally closes a channel that `splitCh` is still responsible for.

### How to call it

```go
package main

import (
	"fmt"
	"sync"
	"time"
)

type transaction struct {
	txID   string
	amount float64
}

// broadcastTx simulates sending a transaction to the network — in
// reality this would be an RPC call to a node, like sendrawtransaction.
func broadcastTx(workerID int, tx transaction) {
	time.Sleep(50 * time.Millisecond) // simulated network latency
	fmt.Printf("worker %d: transaction %s for %.4f BTC broadcast\n", workerID, tx.txID, tx.amount)
}

func main() {
	txs := make(chan transaction)
	go func() {
		defer close(txs)
		for i := 1; i <= 9; i++ {
			txs <- transaction{
				txID:   fmt.Sprintf("tx_%03d", i),
				amount: float64(i) * 0.001,
			}
		}
	}()

	workerChs := splitCh(txs, 3)

	var wg sync.WaitGroup
	wg.Add(len(workerChs))
	for workerID, ch := range workerChs {
		go func(workerID int, ch <-chan transaction) {
			defer wg.Done()
			for tx := range ch {
				broadcastTx(workerID, tx)
			}
		}(workerID, ch)
	}
	wg.Wait()
}
```

Step by step, this is what happens:

1. A goroutine generates nine transactions, `tx_001 .. tx_009`, and writes them into `txs`, closing the channel at the end via `defer`.
2. `splitCh(txs, 3)` starts its own distributor goroutine internally and immediately returns three channels — worker 0 ends up with `tx_001, tx_004, tx_007`, worker 1 with `tx_002, tx_005, tx_008`, worker 2 with `tx_003, tx_006, tx_009`.
3. Each of the three channels gets its own worker goroutine. It reads its slice of transactions through `range` and calls `broadcastTx` for each — the function simulates network latency with `time.Sleep(50 * time.Millisecond)`, as if it were actually hitting a node.
4. `sync.WaitGroup` keeps `main` alive until all three workers have broadcast their share and exited `range` after their channels close.

The payoff shows up in the numbers: nine transactions at 50 milliseconds each is 450 milliseconds if sent one goroutine at a time. With three workers it drops to roughly 150 milliseconds, because the latency of three independent sends overlaps instead of stacking up sequentially.

### Operations

| Function | Signature | What it does |
|---|---|---|
| `splitCh` | `func[T any](inputCh <-chan T, n int) []<-chan T` | splits one channel into N, round-robin |

### When to reach for this pattern

Fits when there's a single data source and genuinely independent work to do on each element — broadcasting transactions to different peers, processing incoming requests, anything where one element can be handled without knowing anything about its neighbors. The more expensive handling a single element is — a network call, heavy computation, a database write — the more the parallelism from fan-out pays off.

Round-robin in `splitCh` doesn't care which worker is free and which is still busy — distribution happens strictly in order regardless of actual load. If workers or elements vary a lot in processing time, some worker ends up idle waiting for its next value while another is still working through its queue. For even load under uneven work, a shared channel that workers pull from as they become ready fits better than one handing out work on a fixed schedule.

For a small number of elements or cheap processing, the pattern isn't worth it — the overhead of goroutines and synchronization outweighs the benefit of parallelism, and a plain loop with no workers at all runs faster.

Fan-out often pairs with fan-in: work gets split across workers, each writes to its own channel, then everything gets merged back into a single stream if a unified result is needed rather than just a side effect like broadcasting to the network, as in the example above.

### Build and test

```bash
go test -race -v ./...
```

The `-race` flag matters here just as much as with fan-in: several workers run in parallel, and any synchronization bug in how work gets distributed across channels will show up under the race detector.