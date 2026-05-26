/**
 * ELVIN CHAT - sw.js
 * Service Worker: Cache First戦略でオフライン対応
 */

'use strict';

const CACHE_NAME = 'elvin-chat-v1';

// キャッシュ対象（静的アセット）
const CACHE_ASSETS = [
  './',
  './index.html',
  './style.css',
  './app.js',
  './manifest.json',
  './icon.svg',
];

// ── インストール ─────────────────────────────────────────────────────────

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => cache.addAll(CACHE_ASSETS))
      .then(() => self.skipWaiting())
  );
});

// ── アクティベート ───────────────────────────────────────────────────────

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(key => key !== CACHE_NAME)
          .map(key => caches.delete(key))
      )
    ).then(() => self.clients.claim())
  );
});

// ── フェッチ戦略 ─────────────────────────────────────────────────────────

self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);

  // VPS API へのリクエストはキャッシュしない（Network Only）
  if (url.hostname === 'api.nihon-neon.jp') {
    event.respondWith(fetch(event.request));
    return;
  }

  // 静的アセット: Cache First
  event.respondWith(
    caches.match(event.request).then(cached => {
      if (cached) return cached;

      return fetch(event.request).then(response => {
        // 成功レスポンスのみキャッシュ
        if (response && response.status === 200 && response.type === 'basic') {
          const toCache = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(event.request, toCache));
        }
        return response;
      }).catch(() => {
        // オフライン時: index.htmlをフォールバック
        if (event.request.destination === 'document') {
          return caches.match('./index.html');
        }
      });
    })
  );
});
