const API_URL = window.location.origin;
let token = localStorage.getItem('token');
const CHAT_USER_PATH = '/chat/user';
const CHAT_ADMIN_PATH = '/chat/admin';
const CURRENT_PATH = window.location.pathname.replace(/\/+$/, '') || '/';
const PAGE_MODE = CURRENT_PATH.startsWith(CHAT_ADMIN_PATH) ? 'admin' : 'user';

// URLのtokenを取り込み、クエリから除去（無限リダイレクト防止）
const bootParams = new URLSearchParams(window.location.search);
const urlTokens = bootParams.getAll('token')
    .map((v) => String(v || '').trim())
    .filter(Boolean);
if (urlTokens.length) {
    token = urlTokens[urlTokens.length - 1];
    localStorage.setItem('token', token);
    localStorage.setItem('token_time', Date.now().toString());
    bootParams.delete('token');
    const nextQuery = bootParams.toString();
    const nextUrl = `${window.location.pathname}${nextQuery ? `?${nextQuery}` : ''}${window.location.hash || ''}`;
    window.history.replaceState({}, document.title, nextUrl);
}

if (!token) {
    // tokenクエリはauth redirectへ持ち越さない
    const redirectParams = new URLSearchParams(window.location.search);
    redirectParams.delete('token');
    const redirectQuery = redirectParams.toString();
    const redirectTarget = `${window.location.pathname}${redirectQuery ? `?${redirectQuery}` : ''}`;
    window.location.replace(`/auth.html?redirect=${encodeURIComponent(redirectTarget)}`);
}

const CATEGORY_LABELS = {
    bug_report: 'バグ報告',
    question: '質問',
    inquiry: 'お問い合わせ'
};

const STATUS_LABELS = {
    open: '受付中',
    waiting_admin: '運営対応待ち',
    waiting_user: 'ユーザー返信待ち',
    closed: '完了'
};
const DISPLAY_LOCALE = 'ja-JP';
const DISPLAY_TIME_ZONE = 'Asia/Tokyo';

const state = {
    user: null,
    isAdmin: false,
    adminView: false,
    scope: 'mine',
    tickets: [],
    filteredTickets: [],
    activeTicketId: null,
    isDraftTicket: true,
    historyQuery: '',
    messageCache: new Map()
};

let refreshTimer = null;
let searchTimer = null;
let toastTimer = null;

function escapeHtml(value) {
    const div = document.createElement('div');
    div.textContent = value == null ? '' : String(value);
    return div.innerHTML;
}

function parseDate(value) {
    if (!value) return null;
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? null : date;
}

function formatDate(value) {
    const date = parseDate(value);
    if (!date) return '-';
    return date.toLocaleString(DISPLAY_LOCALE, { timeZone: DISPLAY_TIME_ZONE, hour12: false });
}

function formatRelative(value) {
    const date = parseDate(value);
    if (!date) return '-';
    const diffSec = Math.floor((Date.now() - date.getTime()) / 1000);
    if (diffSec < 60) return 'たった今';
    if (diffSec < 3600) return `${Math.floor(diffSec / 60)}分前`;
    if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}時間前`;
    if (diffSec < 86400 * 7) return `${Math.floor(diffSec / 86400)}日前`;
    return formatDate(value).slice(0, 10);
}

function formatStatus(status) {
    return STATUS_LABELS[status] || status || '-';
}

function statusClass(status) {
    if (status === 'waiting_admin') return 'waiting_admin';
    if (status === 'waiting_user') return 'waiting_user';
    if (status === 'closed') return 'closed';
    return 'open';
}

function ticketUpdatedAt(ticket) {
    return ticket?.last_message_at || ticket?.updated_at || ticket?.created_at || '';
}

function showToast(message) {
    const el = document.getElementById('chat-toast');
    if (!el) return;
    el.textContent = message;
    el.classList.add('show');
    if (toastTimer) clearTimeout(toastTimer);
    toastTimer = setTimeout(() => {
        el.classList.remove('show');
    }, 2200);
}

async function api(endpoint, options = {}) {
    const headers = {
        Authorization: `Bearer ${token}`,
        ...options.headers
    };

    if (!(options.body instanceof FormData)) {
        headers['Content-Type'] = headers['Content-Type'] || 'application/json';
    }

    const response = await fetch(`${API_URL}${endpoint}`, {
        ...options,
        headers
    });

    const contentType = response.headers.get('content-type') || '';
    const payload = contentType.includes('application/json')
        ? await response.json().catch(() => ({}))
        : {};

    if (!response.ok) {
        throw new Error(payload?.error || `HTTP ${response.status}`);
    }

    return payload;
}

function getTicketById(ticketId) {
    return state.tickets.find((ticket) => ticket.id === ticketId) || null;
}

function getPreferredTicketFromUrl() {
    const params = new URLSearchParams(window.location.search);
    const ticket = String(params.get('ticket') || '').trim();
    return ticket || null;
}

function setUserMeta() {
    const userEl = document.getElementById('chat-user');
    const countEl = document.getElementById('chat-ticket-count');

    if (userEl && state.user) {
        const role = state.isAdmin ? '管理者' : 'ユーザー';
        userEl.textContent = `${state.user.name || state.user.email || 'user'} (${role})`;
    }

    if (countEl) {
        const countSource = state.adminView ? state.tickets.length : (state.activeTicketId ? 1 : 0);
        countEl.textContent = `${countSource}件`;
    }
}

function applyHistoryFilter() {
    if (!state.adminView) {
        state.filteredTickets = [];
        return;
    }

    const q = state.historyQuery.trim().toLowerCase();
    if (!q) {
        state.filteredTickets = [...state.tickets];
        return;
    }

    state.filteredTickets = state.tickets.filter((ticket) => {
        const hay = [ticket.id, ticket.subject, ticket.last_message, ticket.user_name, ticket.user_email]
            .join(' ')
            .toLowerCase();
        return hay.includes(q);
    });
}

function renderTicketList() {
    const listEl = document.getElementById('chat-ticket-list');
    if (!listEl) return;

    if (!state.adminView) {
        listEl.innerHTML = '<div class="chat-empty">履歴は表示されません</div>';
        return;
    }

    const items = [];
    items.push(`
        <button type="button" class="chat-ticket-item new ${state.isDraftTicket ? 'active' : ''}" data-new-ticket="1">
            <div class="chat-ticket-title">新しいチャット</div>
            <div class="chat-ticket-preview">管理者として新規問い合わせ起点を開く</div>
        </button>
    `);

    if (!state.filteredTickets.length) {
        items.push('<div class="chat-empty">履歴がありません</div>');
    } else {
        for (const ticket of state.filteredTickets) {
            const active = !state.isDraftTicket && ticket.id === state.activeTicketId ? 'active' : '';
            const status = statusClass(ticket.status);
            const preview = escapeHtml((ticket.last_message || '').trim() || '（本文なし）');
            const updated = formatRelative(ticketUpdatedAt(ticket));
            items.push(`
                <button type="button" class="chat-ticket-item ${active}" data-ticket-id="${escapeHtml(ticket.id)}">
                    <div class="chat-ticket-title">${escapeHtml(ticket.subject || '(無題)')}</div>
                    <div class="chat-ticket-id">${escapeHtml(ticket.id)}</div>
                    <div class="chat-ticket-meta">
                        <span class="chat-status status-${status}">${escapeHtml(formatStatus(ticket.status))}</span>
                        <span>${escapeHtml(updated)}</span>
                    </div>
                    <div class="chat-ticket-preview">${preview}</div>
                </button>
            `);
        }
    }

    listEl.innerHTML = items.join('');

    listEl.querySelectorAll('[data-ticket-id]').forEach((btn) => {
        btn.addEventListener('click', () => {
            const id = btn.getAttribute('data-ticket-id');
            if (!id) return;
            openTicket(id, { fetchMessages: !state.messageCache.has(id) });
        });
    });

    listEl.querySelector('[data-new-ticket="1"]')?.addEventListener('click', () => {
        startNewDraft();
    });
}

function resetThreadForDraft() {
    state.activeTicketId = null;
    state.isDraftTicket = true;

    const titleEl = document.getElementById('chat-thread-title');
    const metaEl = document.getElementById('chat-thread-meta');
    const statusEl = document.getElementById('chat-thread-status');
    const deleteBtn = document.getElementById('chat-delete-ticket-btn');
    const toggleBtn = document.getElementById('chat-toggle-status-btn');
    const messagesEl = document.getElementById('chat-thread-messages');

    if (titleEl) titleEl.innerHTML = '<i class="fas fa-comment-dots"></i> 新しいチャット';
    if (metaEl) metaEl.textContent = 'メッセージを送ると問い合わせが開始されます';
    if (statusEl) {
        statusEl.className = 'chat-status status-open';
        statusEl.textContent = '新規';
    }

    if (toggleBtn) {
        if (state.adminView) {
            toggleBtn.style.display = '';
            toggleBtn.disabled = true;
            toggleBtn.innerHTML = '<i class="fas fa-check"></i> 完了にする';
        } else {
            toggleBtn.style.display = 'none';
        }
    }

    if (deleteBtn) {
        if (state.adminView) {
            deleteBtn.style.display = '';
            deleteBtn.disabled = true;
            deleteBtn.innerHTML = '<i class="fas fa-trash"></i> 削除';
        } else {
            deleteBtn.style.display = 'none';
        }
    }

    if (messagesEl) {
        messagesEl.innerHTML = '<div class="chat-empty">そのままメッセージを送ると新しい問い合わせが開始されます。</div>';
    }
}

function updateThreadHeader(ticket) {
    const titleEl = document.getElementById('chat-thread-title');
    const metaEl = document.getElementById('chat-thread-meta');
    const statusEl = document.getElementById('chat-thread-status');
    const deleteBtn = document.getElementById('chat-delete-ticket-btn');
    const toggleBtn = document.getElementById('chat-toggle-status-btn');

    if (titleEl) titleEl.innerHTML = `<i class="fas fa-comment-dots"></i> ${escapeHtml(ticket.subject || '(無題)')}`;
    if (metaEl) {
        const category = CATEGORY_LABELS[ticket.category] || ticket.category || '-';
        const owner = state.adminView ? ` / 作成者: ${ticket.user_name || ticket.user_email || '-'}` : '';
        metaEl.textContent = `ID: ${ticket.id} / ${category} / 作成: ${formatDate(ticket.created_at)}${owner}`;
    }
    if (statusEl) {
        const css = statusClass(ticket.status);
        statusEl.className = `chat-status status-${css}`;
        statusEl.textContent = formatStatus(ticket.status);
    }

    if (toggleBtn) {
        if (!state.adminView) {
            toggleBtn.style.display = 'none';
        } else {
            toggleBtn.style.display = '';
            toggleBtn.disabled = false;
            toggleBtn.innerHTML = ticket.status === 'closed'
                ? '<i class="fas fa-rotate-left"></i> 再開する'
                : '<i class="fas fa-check"></i> 完了にする';
        }
    }

    if (deleteBtn) {
        if (!state.adminView) {
            deleteBtn.style.display = 'none';
        } else {
            deleteBtn.style.display = '';
            deleteBtn.disabled = false;
            deleteBtn.innerHTML = '<i class="fas fa-trash"></i> 削除';
        }
    }
}

function focusComposer() {
    const inputEl = document.getElementById('chat-reply-input');
    if (inputEl) inputEl.focus();
}

function startNewDraft() {
    resetThreadForDraft();
    renderTicketList();
    focusComposer();
}

function isNearBottom(el, threshold = 72) {
    if (!el) return true;
    const distance = el.scrollHeight - (el.scrollTop + el.clientHeight);
    return distance <= threshold;
}

function renderMessages(messages, { forceBottom = false } = {}) {
    const messagesEl = document.getElementById('chat-thread-messages');
    if (!messagesEl) return;

    const prevTop = messagesEl.scrollTop;
    const stickToBottom = forceBottom || isNearBottom(messagesEl);

    if (!messages.length) {
        messagesEl.innerHTML = '<div class="chat-empty">まだメッセージはありません</div>';
        return;
    }

    messagesEl.innerHTML = messages.map((msg) => {
        const role = msg.sender_role === 'admin' ? 'admin' : 'user';
        const author = msg.sender_role === 'admin'
            ? `${msg.user_name || msg.user_email || '運営'} (運営)`
            : (msg.user_id === state.user?.id ? 'あなた' : (msg.user_name || msg.user_email || 'ユーザー'));
        return `
            <div class="chat-message ${role}">
                <div class="chat-message-head">
                    <span class="chat-message-author">${escapeHtml(author)}</span>
                    <span>${escapeHtml(formatDate(msg.created_at))}</span>
                </div>
                <div class="chat-message-body">${escapeHtml(msg.message || '')}</div>
            </div>
        `;
    }).join('');

    if (stickToBottom) {
        messagesEl.scrollTop = messagesEl.scrollHeight;
    } else {
        const maxTop = Math.max(0, messagesEl.scrollHeight - messagesEl.clientHeight);
        messagesEl.scrollTop = Math.min(prevTop, maxTop);
    }
}

async function openTicket(ticketId, options = {}) {
    const { fetchMessages = true, forceBottom = false } = options;
    const ticket = getTicketById(ticketId);
    if (!ticket) {
        startNewDraft();
        return;
    }

    state.activeTicketId = ticketId;
    state.isDraftTicket = false;
    updateThreadHeader(ticket);
    renderTicketList();

    try {
        if (fetchMessages || !state.messageCache.has(ticketId)) {
            const data = await api(`/api/support/tickets/${encodeURIComponent(ticketId)}/messages`);
            const latestTicket = data.ticket;
            const messages = Array.isArray(data.messages) ? data.messages : [];
            state.messageCache.set(ticketId, messages);

            const idx = state.tickets.findIndex((row) => row.id === ticketId);
            if (idx >= 0) {
                state.tickets[idx] = { ...state.tickets[idx], ...latestTicket };
            }

            applyHistoryFilter();
            renderTicketList();
            updateThreadHeader(getTicketById(ticketId) || latestTicket);
            setUserMeta();
            renderMessages(messages, { forceBottom });
        } else {
            renderMessages(state.messageCache.get(ticketId) || [], { forceBottom });
        }
        focusComposer();
    } catch (error) {
        showToast(`チケット読み込み失敗: ${error.message}`);
    }
}

async function loadTickets(preferredTicketId = null, forceMessageReload = false, forceBottom = false) {
    const listEl = document.getElementById('chat-ticket-list');
    if (listEl) {
        listEl.innerHTML = '<div class="chat-empty">読み込み中...</div>';
    }

    try {
        const endpoint = state.adminView && state.scope === 'all'
            ? '/api/support/tickets?scope=all'
            : '/api/support/tickets';

        const rows = await api(endpoint);
        state.tickets = Array.isArray(rows) ? rows : [];

        state.tickets.sort((a, b) => {
            const ta = parseDate(ticketUpdatedAt(a))?.getTime() || 0;
            const tb = parseDate(ticketUpdatedAt(b))?.getTime() || 0;
            return tb - ta;
        });

        setUserMeta();

        if (!state.adminView) {
            const active = state.tickets.find((ticket) => ticket.status !== 'closed');
            renderTicketList();
            if (active?.id) {
                await openTicket(active.id, {
                    fetchMessages: forceMessageReload || !state.messageCache.has(active.id),
                    forceBottom
                });
            } else {
                startNewDraft();
            }
            return;
        }

        applyHistoryFilter();

        if (!state.tickets.length) {
            startNewDraft();
            return;
        }

        const pick = preferredTicketId
            || (state.activeTicketId && state.tickets.some((t) => t.id === state.activeTicketId) ? state.activeTicketId : null)
            || state.tickets[0].id;

        await openTicket(pick, {
            fetchMessages: forceMessageReload || !state.messageCache.has(pick),
            forceBottom
        });
    } catch (error) {
        if (listEl) {
            listEl.innerHTML = `<div class="chat-empty">${escapeHtml(error.message)}</div>`;
        }
        startNewDraft();
    }
}

function makeSubjectFromMessage(message) {
    const plain = String(message || '').replace(/\s+/g, ' ').trim();
    if (!plain) return 'お問い合わせ';
    return plain.length > 40 ? `${plain.slice(0, 40)}…` : plain;
}

async function sendMessage() {
    const inputEl = document.getElementById('chat-reply-input');
    const sendBtn = document.getElementById('chat-send-reply-btn');
    const message = String(inputEl?.value || '').trim();
    if (!message) {
        showToast('メッセージを入力してください');
        return;
    }

    if (sendBtn) {
        sendBtn.disabled = true;
        sendBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 送信中...';
    }

    try {
        if (state.isDraftTicket || !state.activeTicketId) {
            const subject = makeSubjectFromMessage(message);
            const created = await api('/api/support/tickets', {
                method: 'POST',
                body: JSON.stringify({
                    category: 'inquiry',
                    subject,
                    message
                })
            });

            if (inputEl) inputEl.value = '';
            state.messageCache.clear();
            await loadTickets(created.id || null, true, true);
            showToast(created.reused ? '進行中チャットに引き継ぎました' : '新しいチャットを開始しました');
        } else {
            await api(`/api/support/tickets/${encodeURIComponent(state.activeTicketId)}/replies`, {
                method: 'POST',
                body: JSON.stringify({ message })
            });

            if (inputEl) inputEl.value = '';
            state.messageCache.delete(state.activeTicketId);
            await loadTickets(state.activeTicketId, true, true);
            showToast('送信しました');
        }
    } catch (error) {
        showToast(`送信失敗: ${error.message}`);
    } finally {
        if (sendBtn) {
            sendBtn.disabled = false;
            sendBtn.innerHTML = '<i class="fas fa-paper-plane"></i> 送信';
        }
        focusComposer();
    }
}

async function toggleStatus() {
    if (!state.adminView || state.isDraftTicket || !state.activeTicketId) return;

    const ticket = getTicketById(state.activeTicketId);
    if (!ticket) return;

    const nextStatus = ticket.status === 'closed' ? 'open' : 'closed';

    try {
        await api(`/api/support/tickets/${encodeURIComponent(state.activeTicketId)}/status`, {
            method: 'PUT',
            body: JSON.stringify({ status: nextStatus })
        });
        state.messageCache.delete(state.activeTicketId);
        await loadTickets(state.activeTicketId, true);
        showToast('ステータスを更新しました');
    } catch (error) {
        showToast(`ステータス更新失敗: ${error.message}`);
    }
}

async function deleteTicket() {
    if (!state.adminView || state.isDraftTicket || !state.activeTicketId) return;

    const ticket = getTicketById(state.activeTicketId);
    if (!ticket) return;

    const confirmed = window.confirm(
        `このチャットを削除しますか？\nID: ${ticket.id}\nこの操作は取り消せません。`
    );
    if (!confirmed) return;

    const deleteBtn = document.getElementById('chat-delete-ticket-btn');
    if (deleteBtn) {
        deleteBtn.disabled = true;
        deleteBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 削除中...';
    }

    const deletingId = state.activeTicketId;
    const fallbackId = state.tickets.find((row) => row.id !== deletingId)?.id || null;

    try {
        await api(`/api/support/tickets/${encodeURIComponent(deletingId)}`, {
            method: 'DELETE'
        });

        state.messageCache.delete(deletingId);
        state.activeTicketId = null;
        state.isDraftTicket = true;

        await loadTickets(fallbackId, false);
        showToast('チャットを削除しました');
    } catch (error) {
        showToast(`削除失敗: ${error.message}`);
        if (deleteBtn) {
            deleteBtn.disabled = false;
            deleteBtn.innerHTML = '<i class="fas fa-trash"></i> 削除';
        }
    }
}

function bindEvents() {
    document.getElementById('chat-refresh-btn')?.addEventListener('click', () => {
        loadTickets(state.activeTicketId, true);
    });

    document.getElementById('chat-new-btn')?.addEventListener('click', () => {
        startNewDraft();
    });

    document.getElementById('chat-history-search')?.addEventListener('input', (e) => {
        const value = String(e.target.value || '');
        if (searchTimer) clearTimeout(searchTimer);
        searchTimer = setTimeout(() => {
            state.historyQuery = value;
            applyHistoryFilter();
            renderTicketList();
        }, 200);
    });

    document.getElementById('chat-send-reply-btn')?.addEventListener('click', sendMessage);

    document.getElementById('chat-reply-input')?.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            sendMessage();
        }
    });

    document.getElementById('chat-toggle-status-btn')?.addEventListener('click', toggleStatus);
    document.getElementById('chat-delete-ticket-btn')?.addEventListener('click', deleteTicket);

    document.getElementById('chat-logout-btn')?.addEventListener('click', () => {
        localStorage.removeItem('token');
        localStorage.removeItem('user');
        window.location.href = '/auth.html';
    });

    document.getElementById('chat-scope')?.addEventListener('change', async (e) => {
        state.scope = e.target.value === 'all' ? 'all' : 'mine';
        state.messageCache.clear();
        await loadTickets(getPreferredTicketFromUrl(), true);
    });
}

async function loadCurrentUser() {
    const me = await api('/api/auth/me');
    state.user = me;
    state.isAdmin = !!me.is_admin;
    localStorage.setItem('user', JSON.stringify(me));

    const appEl = document.querySelector('.chat-app');
    const scopeEl = document.getElementById('chat-scope');
    const newBtn = document.getElementById('chat-new-btn');

    if (PAGE_MODE === 'admin' && !state.isAdmin) {
        window.location.replace(CHAT_USER_PATH);
        return;
    }
    state.adminView = PAGE_MODE === 'admin' && state.isAdmin;

    if (appEl) {
        appEl.classList.toggle('user-mode', !state.adminView);
        appEl.classList.toggle('admin-mode', state.adminView);
    }

    if (scopeEl) {
        scopeEl.style.display = state.adminView ? '' : 'none';
    }
    state.scope = state.adminView ? 'all' : 'mine';

    if (newBtn) {
        newBtn.style.display = state.adminView ? '' : 'none';
    }
}

function startAutoRefresh() {
    if (refreshTimer) clearInterval(refreshTimer);
    refreshTimer = setInterval(() => {
        if (document.visibilityState === 'visible') {
            loadTickets(state.activeTicketId, true);
        }
    }, 30000);
}

async function init() {
    try {
        bindEvents();
        await loadCurrentUser();
        await loadTickets(getPreferredTicketFromUrl(), true);
        focusComposer();
        startAutoRefresh();
    } catch (error) {
        showToast(`初期化失敗: ${error.message}`);
    }
}

document.addEventListener('DOMContentLoaded', init);
