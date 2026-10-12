# moodycamel::ConcurrentQueue

The lock-free queue behind `IndexRing` in `../tables.h`, and the semaphore a `ControlTable`
caller waits on, copied unchanged.

- Source: <https://github.com/cameron314/concurrentqueue>, commit `683b9e3` of 2026-07-11.
- Files: `concurrentqueue.h`, `blockingconcurrentqueue.h`, `lightweightsemaphore.h`, `LICENSE.md`.
- Licence: Simplified BSD or Boost Software License 1.0, at the user's choice; the semaphore in
  `lightweightsemaphore.h` is under the zlib licence. All three are in `LICENSE.md` or the file.

To update, replace the four files with those of a newer commit and change the commit here.
