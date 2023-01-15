#pragma once

// FIXME(shiges): This seems to be a reasonable way to delay instead
// of pause instructions; the latter seems to be a bit heavier than
// I expected. Weird; needs further investigation.
#define DELAY(n)                       \
  do {                                 \
    volatile int x = 0;                \
    for (int i = 0; i < (n); ++i) x++; \
  } while (0)

