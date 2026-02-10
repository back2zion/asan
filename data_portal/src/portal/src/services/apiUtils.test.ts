/**
 * apiUtils 단위 테스트
 * sanitizeText, sanitizeHtml, getCsrfToken, fetchPost/Put/Delete, 캐시
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { sanitizeText, sanitizeHtml, getCsrfToken, fetchPost, fetchPut, fetchDelete } from './apiUtils';

// ── sanitizeText ──
describe('sanitizeText', () => {
  it('strips all HTML tags', () => {
    expect(sanitizeText('<b>hello</b>')).toBe('hello');
  });

  it('strips script tags and content', () => {
    const result = sanitizeText('<script>alert("xss")</script>safe');
    expect(result).not.toContain('script');
    expect(result).toContain('safe');
  });

  it('returns plain text unchanged', () => {
    expect(sanitizeText('plain text')).toBe('plain text');
  });

  it('handles empty string', () => {
    expect(sanitizeText('')).toBe('');
  });
});

// ── sanitizeHtml ──
describe('sanitizeHtml', () => {
  it('allows safe markdown tags', () => {
    const html = '<p>hello <strong>world</strong></p>';
    const result = sanitizeHtml(html);
    expect(result).toContain('<p>');
    expect(result).toContain('<strong>');
  });

  it('strips script tags', () => {
    const result = sanitizeHtml('<p>ok</p><script>alert(1)</script>');
    expect(result).not.toContain('<script>');
    expect(result).toContain('<p>ok</p>');
  });

  it('strips disallowed tags like iframe', () => {
    const result = sanitizeHtml('<iframe src="evil.com"></iframe><p>safe</p>');
    expect(result).not.toContain('<iframe');
    expect(result).toContain('<p>safe</p>');
  });

  it('allows href and target attributes on links', () => {
    const result = sanitizeHtml('<a href="https://example.com" target="_blank">link</a>');
    expect(result).toContain('href="https://example.com"');
    expect(result).toContain('target="_blank"');
  });

  it('strips onclick and other event attributes', () => {
    const result = sanitizeHtml('<a href="#" onclick="alert(1)">click</a>');
    expect(result).not.toContain('onclick');
  });
});

// ── getCsrfToken ──
describe('getCsrfToken', () => {
  const originalCookie = Object.getOwnPropertyDescriptor(document, 'cookie');

  afterEach(() => {
    if (originalCookie) {
      Object.defineProperty(document, 'cookie', originalCookie);
    }
  });

  it('returns CSRF token from cookie', () => {
    Object.defineProperty(document, 'cookie', {
      get: () => 'other=abc; csrf_token=test-token-123; session=xyz',
      configurable: true,
    });
    expect(getCsrfToken()).toBe('test-token-123');
  });

  it('returns empty string when no CSRF cookie', () => {
    Object.defineProperty(document, 'cookie', {
      get: () => 'session=abc',
      configurable: true,
    });
    expect(getCsrfToken()).toBe('');
  });

  it('returns empty string for empty cookies', () => {
    Object.defineProperty(document, 'cookie', {
      get: () => '',
      configurable: true,
    });
    expect(getCsrfToken()).toBe('');
  });
});

// ── fetchPost / fetchPut / fetchDelete ──
describe('fetchPost', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('{}', { status: 200 })));
    // No CSRF cookie
    Object.defineProperty(document, 'cookie', {
      get: () => '',
      configurable: true,
    });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('sends POST with JSON body', async () => {
    await fetchPost('/api/test', { key: 'value' });
    expect(fetch).toHaveBeenCalledWith('/api/test', expect.objectContaining({
      method: 'POST',
      body: JSON.stringify({ key: 'value' }),
    }));
  });

  it('includes Content-Type application/json', async () => {
    await fetchPost('/api/test', {});
    const call = vi.mocked(fetch).mock.calls[0];
    expect(call[1]?.headers).toEqual(expect.objectContaining({
      'Content-Type': 'application/json',
    }));
  });

  it('includes CSRF header when cookie present', async () => {
    Object.defineProperty(document, 'cookie', {
      get: () => 'csrf_token=my-csrf-token',
      configurable: true,
    });
    await fetchPost('/api/test', {});
    const call = vi.mocked(fetch).mock.calls[0];
    expect(call[1]?.headers).toEqual(expect.objectContaining({
      'X-CSRF-Token': 'my-csrf-token',
    }));
  });
});

describe('fetchPut', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('{}', { status: 200 })));
    Object.defineProperty(document, 'cookie', { get: () => '', configurable: true });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('sends PUT with JSON body', async () => {
    await fetchPut('/api/update', { name: 'test' });
    expect(fetch).toHaveBeenCalledWith('/api/update', expect.objectContaining({
      method: 'PUT',
      body: JSON.stringify({ name: 'test' }),
    }));
  });
});

describe('fetchDelete', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('{}', { status: 200 })));
    Object.defineProperty(document, 'cookie', { get: () => '', configurable: true });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('sends DELETE without body', async () => {
    await fetchDelete('/api/item/1');
    expect(fetch).toHaveBeenCalledWith('/api/item/1', expect.objectContaining({
      method: 'DELETE',
    }));
  });
});
