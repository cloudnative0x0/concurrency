# Fan-In

<p style="text-align: left">
  <a href="#русский">Русский</a> ・ <a href="#english">English</a>
</p>

---

## Русский

Fan-in — паттерн, обратный fan-out: несколько независимых входных каналов сводятся в один выходной. Источники пишут параллельно и никак друг с другом не согласованы, а на выходе получается один поток, который можно вычитывать обычным `range`, не открывая `select` на каждый канал вручную.

В `ExampleFanIn` одна горутина пишет по кругу в три канала — `ch1`, `ch2`, `ch3` — значения `i`, `i+1`, `i+2`, и в конце закрывает все три. `mergeChs` в это время параллельно читает из всех трёх и перекладывает всё в общий выходной канал. Какое значение из какого канала придёт первым — заранее не известно, три горутины внутри `mergeChs` конкурируют за запись в один и тот же `outputCh`.

### Как это устроено

`mergeChs[T any](channels ...<-chan T) <-chan T` заводит `sync.WaitGroup` на количество входных каналов и общий `outputCh`. На каждый входной канал запускается своя горутина: она читает `ch` циклом `range` и пишет прочитанное в `outputCh`, а когда `range` доходит до закрытого входа — горутина вызывает `wg.Done()` и завершается.

Отдельная горутина ждёт `wg.Wait()` и только после этого закрывает `outputCh`. Это единственное место, которое трогает выходной канал на закрытие, поэтому гонки за `close` нет — пишут в канал многие горутины, а закрывает его одна, и только когда все входные точно домолчали.

Отсюда прямое следствие: `mergeChs` сам не закрывает входные каналы — это ответственность вызывающего кода. Если хотя бы один из `ch1, ch2, ch3` не закрыть, соответствующая горутина внутри `mergeChs` навсегда останется на `range`, `wg.Wait()` не дождётся, и `outputCh` не закроется никогда — цикл `for val := range mergeChs(...)` в `ExampleFanIn` просто зависнет.

### Как вызывать

```go
func ExampleFanIn() {
	btcFeed := make(chan int)
	ethFeed := make(chan int)
	solFeed := make(chan int)

	go func() {
		defer close(btcFeed)
		for _, price := range []int{42000, 42150, 41980} {
			btcFeed <- price
		}
	}()

	go func() {
		defer close(ethFeed)
		for _, price := range []int{2200, 2215} {
			ethFeed <- price
		}
	}()

	go func() {
		defer close(solFeed)
		for _, price := range []int{95, 97, 96, 94} {
			solFeed <- price
		}
	}()

	var total int
	for price := range mergeChs(btcFeed, ethFeed, solFeed) {
		total += price
	}

	fmt.Println(total) // Output: 130927
}
```

Число каналов на входе произвольное — `mergeChs(ch1)` работает как проброс одного канала, `mergeChs()` без аргументов сразу отдаёт закрытый канал, потому что `wg.Add(0)` завершается мгновенно.

### Операции

| Функция | Сигнатура | Что делает |
|---|---|---|
| `mergeChs` | `func[T any](channels ...<-chan T) <-chan T` | сводит N независимых каналов в один |

### Когда брать этот паттерн

Подходит, когда есть несколько источников, работающих сами по себе — воркеры из fan-out стадии, параллельные запросы к разным API, чтение из нескольких сокетов — и их результаты нужно обработать одним циклом, а не городить `select` на N веток вручную. Обычно fan-in ставят сразу после fan-out: разбили работу на воркеров, каждый пишет в свой канал, потом свели всё обратно в один поток для дальнейшей обработки.

Гарантий порядка между источниками паттерн не даёт — какая горутина первой успела написать в `outputCh`, то значение и ушло следующим. Если порядок важен, скажем, нужно сохранить, из какого именно канала пришло значение или в какой последовательности, plain fan-in не подойдёт, придётся оборачивать значения в структуру с меткой источника или использовать другой подход.

Для одного источника паттерн избыточен — это просто лишняя горутина поверх уже существующего канала. И, как уже сказано, вызывающий код обязан закрыть все входные каналы сам, иначе часть горутин внутри `mergeChs` зависнет на чтении навсегда и произойдёт утечка.

Флаг `-race` тут не для галочки: несколько горутин пишут в один и тот же `outputCh`, и любая ошибка в синхронизации всплывёт именно под race detector'ом, а не в обычном прогоне.

---

## English

Fan-in is the inverse of fan-out: several independent input channels get merged into one output channel. Sources write in parallel with no coordination between them, and the caller gets back a single stream it can read with a plain `range` instead of hand-rolling a `select` over every channel.

In `ExampleFanIn` one goroutine writes round-robin into three channels — `ch1`, `ch2`, `ch3` — values `i`, `i+1`, `i+2` — and closes all three at the end. `mergeChs` reads from all three at the same time and forwards everything into a shared output channel. Which value from which channel lands first isn't defined — the three goroutines inside `mergeChs` are racing to write into the same `outputCh`.

### How it's built

`mergeChs[T any](channels ...<-chan T) <-chan T` sets up a `sync.WaitGroup` sized to the number of input channels plus a shared `outputCh`. Each input channel gets its own goroutine: it reads `ch` through `range` and writes what it reads into `outputCh`, and once `range` hits a closed input, the goroutine calls `wg.Done()` and exits.

A separate goroutine waits on `wg.Wait()` and only then closes `outputCh`. That's the only place touching the output channel's close, so there's no race on `close` itself — many goroutines write to the channel, but only one closes it, and only once every input has genuinely gone quiet.

The direct consequence: `mergeChs` never closes the input channels itself — that's on the caller. Leave even one of `ch1, ch2, ch3` open and the matching goroutine inside `mergeChs` sits on `range` forever, `wg.Wait()` never returns, `outputCh` never closes — the `for val := range mergeChs(...)` loop in `ExampleFanIn` just hangs.

### How to call it

```go
func ExampleFanIn() {
	btcFeed := make(chan int)
	ethFeed := make(chan int)
	solFeed := make(chan int)

	go func() {
		defer close(btcFeed)
		for _, price := range []int{42000, 42150, 41980} {
			btcFeed <- price
		}
	}()

	go func() {
		defer close(ethFeed)
		for _, price := range []int{2200, 2215} {
			ethFeed <- price
		}
	}()

	go func() {
		defer close(solFeed)
		for _, price := range []int{95, 97, 96, 94} {
			solFeed <- price
		}
	}()

	var total int
	for price := range mergeChs(btcFeed, ethFeed, solFeed) {
		total += price
	}

	fmt.Println(total) // Output: 130927
}
```

The number of input channels is arbitrary — `mergeChs(ch1)` behaves like a passthrough for a single channel, and `mergeChs()` with no arguments returns an already-closed channel right away, since `wg.Add(0)` finishes instantly.

### Operations

| Function | Signature | What it does |
|---|---|---|
| `mergeChs` | `func[T any](channels ...<-chan T) <-chan T` | converges N independent channels into one |

### When to reach for this pattern

Fits when there are several sources doing their own thing independently — workers coming out of a fan-out stage, parallel calls to different APIs, reads off several sockets — and their results need to be processed in one loop instead of hand-building a `select` with N branches. Fan-in usually sits right after fan-out: work got split across workers, each writes to its own channel, then everything gets merged back into a single stream for whatever comes next.

The pattern gives no ordering guarantees between sources — whichever goroutine wins the race to write into `outputCh` is what comes out next. If order matters, say you need to know which channel a value came from or preserve some sequence, plain fan-in won't do it — values need to be wrapped with a source tag, or a different approach is needed.

For a single source the pattern is overkill — it's just an extra goroutine wrapped around a channel that already exists. And as already noted, the caller is on the hook for closing every input channel, otherwise some of the goroutines inside `mergeChs` sit on a read forever and leak.

The `-race` flag here isn't decorative: multiple goroutines write into the same `outputCh`, and any synchronization bug shows up under the race detector, not in a plain run.