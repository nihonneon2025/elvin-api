/**
 * ELVIN CHAT - app.js
 * メインロジック: メッセージ送信 → VPS APIポーリング → 結果表示
 */

'use strict';

// ── 設定 ────────────────────────────────────────────────────────────────

const CONFIG = {
  VPS_BASE_URL:     'https://api.nihon-neon.jp',
  POLL_INTERVAL_MS: 3000,
  TIMEOUT_MS:       120000,
  STORAGE_KEY_NAME: 'elvin_chat_sender_name',
  STORAGE_KEY_HIST: 'elvin_chat_history',
};

// ── 状態 ────────────────────────────────────────────────────────────────

const state = {
  token:       null,   // URLから取得したclient_token
  senderName:  '',     // スタッフ名（LocalStorage保存）
  agents:      [],     // エージェント一覧
  selectedAgent: null, // 現在選択中のエージェントID
  isProcessing: false, // タスク処理中フラグ
  pollTimer:   null,   // ポーリングタイマー
  history:     [],     // チャット履歴（セッション中のみ）
};

// ── DOM参照 ─────────────────────────────────────────────────────────────

const $ = id => document.getElementById(id);

const dom = {
  app:           $('app'),
  chatArea:      $('chat-area'),
  emptyState:    $('empty-state'),
  typingIndicator: $('typing-indicator'),
  typingLabel:   $('typing-label'),
  messageInput:  $('message-input'),
  sendBtn:       $('send-btn'),
  agentSelect:   $('agent-select'),
  agentBadge:    $('agent-badge-name'),
  errorBanner:   $('error-banner'),
  nameModal:     $('name-modal'),
  nameInput:     $('name-input'),
  nameSubmit:    $('name-submit'),
};

// ── 初期化 ──────────────────────────────────────────────────────────────

async function init() {
  // URLからclient_tokenを取得
  const params = new URLSearchParams(window.location.search);
  state.token = params.get('token') || '';

  if (!state.token) {
    showError('URLにtokenパラメータがありません。例: ?token=xxxx');
    disableInput();
    return;
  }

  // スタッフ名の確認
  const savedName = localStorage.getItem(CONFIG.STORAGE_KEY_NAME);
  if (savedName) {
    state.senderName = savedName;
    hideNameModal();
    await loadAgents();
  } else {
    showNameModal();
  }

  // 入力フォームのイベント
  dom.messageInput.addEventListener('keydown', onInputKeydown);
  dom.messageInput.addEventListener('input', autoResizeInput);
  dom.sendBtn.addEventListener('click', onSendClick);
  dom.agentSelect.addEventListener('change', onAgentChange);
  dom.nameSubmit.addEventListener('click', onNameSubmit);
  dom.nameInput.addEventListener('keydown', e => {
    if (e.key === 'Enter') onNameSubmit();
  });
}

// ── エージェント取得 ─────────────────────────────────────────────────────

async function loadAgents() {
  try {
    const res = await apiFetch(`/api/v1/chat/agents`);
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      showError(`認証エラー: ${err.error || res.status}。URLのtokenを確認してください。`);
      disableInput();
      return;
    }
    state.agents = await res.json();

    // セレクトボックス更新
    dom.agentSelect.innerHTML = '';
    if (state.agents.length === 0) {
      const opt = document.createElement('option');
      opt.value = '';
      opt.textContent = 'エージェント未設定';
      dom.agentSelect.appendChild(opt);
    } else {
      state.agents.forEach(ag => {
        const opt = document.createElement('option');
        opt.value = ag.id;
        opt.textContent = ag.name + (ag.role ? ` (${ag.role})` : '');
        dom.agentSelect.appendChild(opt);
      });
      state.selectedAgent = state.agents[0].id;
      updateAgentBadge(state.agents[0].name);
    }

    hideError();
  } catch (e) {
    showError('VPSへの接続に失敗しました。ネットワークを確認してください。');
    disableInput();
  }
}

// ── 名前モーダル ─────────────────────────────────────────────────────────

function showNameModal() {
  dom.nameModal.classList.remove('hidden');
  setTimeout(() => dom.nameInput.focus(), 100);
}

function hideNameModal() {
  dom.nameModal.classList.add('hidden');
}

async function onNameSubmit() {
  const name = dom.nameInput.value.trim();
  if (!name) {
    dom.nameInput.style.borderColor = 'var(--error)';
    return;
  }
  dom.nameInput.style.borderColor = '';
  state.senderName = name;
  localStorage.setItem(CONFIG.STORAGE_KEY_NAME, name);
  hideNameModal();
  await loadAgents();
}

// ── エージェント切替 ─────────────────────────────────────────────────────

function onAgentChange() {
  state.selectedAgent = dom.agentSelect.value;
  const ag = state.agents.find(a => a.id === state.selectedAgent);
  if (ag) updateAgentBadge(ag.name);
}

function updateAgentBadge(name) {
  dom.agentBadge.textContent = name;
}

// ── メッセージ送信 ───────────────────────────────────────────────────────

function onInputKeydown(e) {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    onSendClick();
  }
}

function autoResizeInput() {
  dom.messageInput.style.height = 'auto';
  dom.messageInput.style.height = Math.min(dom.messageInput.scrollHeight, 120) + 'px';
}

async function onSendClick() {
  if (state.isProcessing) return;
  const text = dom.messageInput.value.trim();
  if (!text) return;

  // UI更新
  dom.messageInput.value = '';
  dom.messageInput.style.height = 'auto';
  hideEmptyState();
  hideError();

  // ユーザーメッセージ表示
  appendMessage('user', text, state.senderName);

  // 処理中状態
  setProcessing(true);

  try {
    await sendAndPoll(text);
  } catch (e) {
    appendMessage('ai', `エラーが発生しました: ${e.message}`, null, true);
  } finally {
    setProcessing(false);
  }
}

// ── API送信 → ポーリング ─────────────────────────────────────────────────

async function sendAndPoll(text) {
  // タスク投入
  const body = {
    message: text,
    sender: state.senderName,
  };
  if (state.selectedAgent) {
    body.agent_id = state.selectedAgent;
  }

  const res = await apiFetch('/api/v1/chat/send', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error || `HTTP ${res.status}`);
  }

  const { task_id, agent_name } = await res.json();

  // スピナー表示（エージェント名を使用）
  const displayName = agent_name || 'ELVIN';
  showTyping(`${displayName} が作業中`);

  // ポーリング
  const result = await pollTask(task_id);
  hideTyping();

  if (result.status === 'completed') {
    const output = extractOutput(result.result);
    appendMessage('ai', output, displayName);
  } else {
    const errMsg = result.error || result.result?.error || '処理に失敗しました';
    appendMessage('ai', `処理に失敗しました: ${errMsg}`, displayName, true);
  }
}

async function pollTask(taskId) {
  const deadline = Date.now() + CONFIG.TIMEOUT_MS;

  while (Date.now() < deadline) {
    await sleep(CONFIG.POLL_INTERVAL_MS);

    const res = await apiFetch(`/api/v1/chat/tasks/${taskId}`);
    if (!res.ok) {
      throw new Error(`ポーリングエラー: HTTP ${res.status}`);
    }

    const data = await res.json();
    if (data.status === 'completed' || data.status === 'failed') {
      return data;
    }
    // pending / running は継続
  }

  throw new Error(`タイムアウト: ${CONFIG.TIMEOUT_MS / 1000}秒以内に完了しませんでした`);
}

function extractOutput(result) {
  if (!result) return '（結果なし）';
  if (typeof result === 'string') return result;

  // よくあるフィールド名を順に確認
  return result.output
    || result.reply
    || result.message
    || result.text
    || result.content
    || JSON.stringify(result, null, 2);
}

// ── UI ヘルパー ──────────────────────────────────────────────────────────

function appendMessage(type, text, senderLabel, isError = false) {
  // 空状態を隠す
  hideEmptyState();

  const wrap = document.createElement('div');
  wrap.className = `message ${type}`;

  // 送信者・時刻
  const meta = document.createElement('div');
  meta.className = 'meta';
  const label = senderLabel || (type === 'user' ? state.senderName : 'ELVIN');
  meta.textContent = `${label}  ${formatTime(new Date())}`;
  wrap.appendChild(meta);

  // 吹き出し
  const bubble = document.createElement('div');
  bubble.className = 'bubble' + (isError ? ' error' : '');
  bubble.textContent = text;
  wrap.appendChild(bubble);

  dom.chatArea.appendChild(wrap);
  scrollToBottom();

  // 履歴に追加
  state.history.push({ type, text, senderLabel: label, time: new Date().toISOString() });
}

function showTyping(label) {
  dom.typingLabel.textContent = label + '...';
  dom.typingIndicator.classList.add('visible');
  scrollToBottom();
}

function hideTyping() {
  dom.typingIndicator.classList.remove('visible');
}

function setProcessing(flag) {
  state.isProcessing = flag;
  dom.sendBtn.disabled = flag;
  dom.messageInput.disabled = flag;
  if (!flag) {
    dom.messageInput.focus();
    if (!dom.typingIndicator.classList.contains('visible')) {
      hideTyping();
    }
  }
}

function hideEmptyState() {
  if (dom.emptyState) dom.emptyState.style.display = 'none';
}

function showError(msg) {
  dom.errorBanner.textContent = msg;
  dom.errorBanner.classList.add('visible');
}

function hideError() {
  dom.errorBanner.classList.remove('visible');
}

function disableInput() {
  dom.messageInput.disabled = true;
  dom.sendBtn.disabled = true;
  dom.messageInput.placeholder = '利用できません';
}

function scrollToBottom() {
  requestAnimationFrame(() => {
    dom.chatArea.scrollTop = dom.chatArea.scrollHeight;
  });
}

function formatTime(date) {
  return date.toLocaleTimeString('ja-JP', { hour: '2-digit', minute: '2-digit' });
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ── API 共通フェッチ ────────────────────────────────────────────────────

function apiFetch(path, options = {}) {
  const url = CONFIG.VPS_BASE_URL + path;
  const headers = {
    'X-Client-Token': state.token,
    ...(options.headers || {}),
  };
  return fetch(url, { ...options, headers });
}

// ── エントリポイント ─────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', init);
