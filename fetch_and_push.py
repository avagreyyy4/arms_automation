#!/usr/bin/env python3
# Run: HEADLESS=false python fetch.py

import asyncio, json, os, re, tempfile
from pathlib import Path
from typing import Dict, List, Optional
import os
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ===================== ENV =====================
from dotenv import load_dotenv
load_dotenv()  # local dev convenience; no-op on Cloud Run

import os
import gspread
from google.oauth2.service_account import Credentials

os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", "/var/secrets/google/SHEETS_SA_JSON")
SA_PATH   = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "sa.json")

ARMS_USER = os.getenv("ARMS_USERNAME") or os.getenv("ARMS_USER")
ARMS_PASS = os.getenv("ARMS_PASSWORD") or os.getenv("ARMS_PASS")
ARMS_BASE = (os.getenv("ARMS_BASE_URL") or "").rstrip("/")
ARMS_LOGIN_URL = os.getenv("ARMS_LOGIN_URL") or (f"{ARMS_BASE}/login" if ARMS_BASE else None)
SHEET_ID  = os.getenv("SHEET_ID")  # we’ll set this as a normal env var on deploy
HEADLESS  = (os.getenv("HEADLESS", "true").lower() != "false")

missing = []
if not ARMS_USER: missing.append("ARMS_USERNAME/ARMS_USER")
if not ARMS_PASS: missing.append("ARMS_PASSWORD/ARMS_PASS")
if not (ARMS_BASE or ARMS_LOGIN_URL): missing.append("ARMS_BASE_URL or ARMS_LOGIN_URL")
if not SHEET_ID:  missing.append("SHEET_ID")
if not SA_PATH:   missing.append("GOOGLE_APPLICATION_CREDENTIALS/sa.json")
if missing:
    raise SystemExit(f"[fatal] Missing required env: {', '.join(missing)}")
if not ARMS_LOGIN_URL:
    ARMS_LOGIN_URL = f"{ARMS_BASE}/login"
    
# ===================== SHEETS HELPERS =====================
def _gs_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(SA_PATH, scopes=scopes)
    return gspread.authorize(creds)

def overwrite_tab(df: pd.DataFrame, tab_name: str):
    gc = _gs_client()
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=100, cols=26)
    ws.clear()
    df.columns = [str(c) for c in df.columns]
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)

# ===================== UTILS / CACHE =====================
CACHE_PATH = Path(__file__).with_name(".exports_cache.json")

def _read_cache():
    try:
        return json.loads(CACHE_PATH.read_text())
    except Exception:
        return {}

def _write_cache(d):
    try:
        CACHE_PATH.write_text(json.dumps(d, indent=2))
    except Exception:
        pass

def _rx_exact(s: str):
    return re.compile(rf"^{re.escape(s)}$", re.I)

def _rx_startswith(s: str):
    return re.compile(rf"^\s*{re.escape(s)}\b", re.I)

def _layout_tokens(layout_text: str):
    toks = re.findall(r"[A-Za-z0-9]+", layout_text.lower())
    stop = {"the","of","for","and","a","an","to","by","in","on"}
    return [t for t in toks if t not in stop]

def _filename_matches_layout(fn: str, tokens):
    s = re.sub(r"[^a-z0-9]+", " ", fn.lower())
    return all(t in s for t in tokens)

# ===================== NAVIGATION / FILTERS =====================
import re, asyncio

def _rx_exact(s: str):
    return re.compile(rf"^\s*{re.escape(s)}\s*$", re.I)

async def click_recruiting_recruits(page):
    """From Dashboard left nav, open Recruiting → Recruits."""
    # Open/ensure the left drawer is visible (some tenants hide it)
    try:
        # If there's a burger/chevron for the main nav, click it once
        chevron = page.locator("button, [role='button']").filter(
            has_text=re.compile(r"^\s*Close Menu|Open Menu\s*$", re.I)
        ).first
        if await chevron.count():
            try: await chevron.click(timeout=800)
            except: pass
    except: pass

    # Click "Recruiting" in the left rail
    for loc in [
        page.get_by_role("link", name=_rx_exact("Recruiting")).first,
        page.get_by_role("button", name=_rx_exact("Recruiting")).first,
        page.locator("nav,aside").get_by_text(_rx_exact("Recruiting")).first,
        # Fallback to the icon-only entry (SVG id has 'recruiting-icon')
        page.locator("nav svg use[href*='recruiting-icon'], nav svg use[xlink\\:href*='recruiting-icon']").first
    ]:
        try:
            await loc.scroll_into_view_if_needed(); await loc.click(timeout=3000); break
        except: continue
    else:
        raise RuntimeError("Could not find 'Recruiting' in left navigation.")

    # Click "Recruits" in the flyout/submenu
    for loc in [
        page.get_by_role("link", name=_rx_exact("Recruits")).first,
        page.get_by_role("menuitem", name=_rx_exact("Recruits")).first,
        page.get_by_text(_rx_exact("Recruits")).first,
    ]:
        try:
            await loc.click(timeout=4000); break
        except: continue
    else:
        raise RuntimeError("Could not click ‘Recruits’ in the flyout.")

    await page.wait_for_load_state("networkidle")


async def _expand_section(scope, title_regex):
    try:
        hdr = scope.get_by_role("button", name=title_regex).first
        await hdr.wait_for(timeout=1200)
        expanded = await hdr.get_attribute("aria-expanded")
        if expanded is not None and expanded.lower() == "false":
            await hdr.click(); await scope.wait_for_load_state("networkidle"); await asyncio.sleep(0.1); return
    except: pass
    try:
        hdr2 = scope.locator(".mat-expansion-panel-header").filter(has=scope.get_by_text(title_regex)).first
        await hdr2.wait_for(timeout=1200)
        classes = (await hdr2.get_attribute("class")) or ""
        if "mat-expanded" not in classes:
            await hdr2.click(); await asyncio.sleep(0.1)
    except: pass

async def _click_link_in_section(scope, section_title_rx, link_text_rx):
    sec = scope.locator("section,div,aside").filter(has=scope.get_by_text(section_title_rx)).first
    try: await sec.wait_for(timeout=1000)
    except: sec = scope
    for loc in [sec.get_by_role("link", name=link_text_rx).first, sec.get_by_text(link_text_rx).first]:
        try:
            await loc.wait_for(timeout=600); await loc.click(); await asyncio.sleep(0.05); return True
        except: continue
    return False

async def _scroll_until_visible(scope, regex, max_steps=20):
    container = scope.locator(".mat-drawer-content, .mat-sidenav-content, .cdk-virtual-scroll-viewport").first
    for _ in range(max_steps):
        try:
            el = scope.get_by_text(regex).first
            await el.scroll_into_view_if_needed(); await el.wait_for(timeout=300); return True
        except:
            try: await container.evaluate("(el)=>el.scrollBy(0,300)")
            except: await scope.evaluate("()=>window.scrollBy(0,300)")
            await asyncio.sleep(0.05)
    return False

async def ensure_checkbox_checked(scope, name_regex):
    host = scope.locator("mat-checkbox").filter(has=scope.get_by_text(name_regex)).first
    try:
        if await host.count():
            classes = (await host.get_attribute("class")) or ""
            if "mat-checkbox-checked" in classes: return
            for tgt_sel in [".mat-checkbox-inner-container", "label", ".mat-checkbox-layout"]:
                tgt = host.locator(tgt_sel)
                try:
                    await tgt.scroll_into_view_if_needed(); await tgt.click(); return
                except: continue
            await host.click(force=True); return
    except: pass
    try:
        lbl = scope.get_by_label(name_regex).first
        await lbl.scroll_into_view_if_needed()
        try: await lbl.check()
        except: await lbl.click()
        return
    except: pass
    await scope.get_by_text(name_regex).first.click(force=True)

async def find_filters_scope(page):
    try:
        await page.get_by_text(_rx_exact("Grad. Year")).first.wait_for(timeout=1200); return page
    except: pass
    for fr in page.frames:
        try:
            await fr.get_by_text(_rx_exact("Grad. Year")).first.wait_for(timeout=800); return fr
        except: continue
    return page
    
def _parse_statuses(exp: Dict) -> List[str]:
    f = exp.get("filters") or {}
    s = f.get("status", {})  # e.g., {"values": ["Prospect","Committed"]}
    vals = s.get("values") or []
    if isinstance(vals, str):
        vals = [v.strip() for v in re.split(r"[,\|/]+", vals) if v.strip()]
    return vals
    
async def apply_filters(scope, grad_year: Optional[str], statuses: Optional[List[str]] = None):
    await _expand_section(scope, _rx_exact("Status"))
    await _expand_section(scope, _rx_exact("Grad. Year"))

    # --- STATUS (optional explicit select) ---
    if statuses:
        await _click_link_in_section(scope, _rx_exact("Status"), _rx_exact("none"))
        for s in statuses:
            await ensure_checkbox_checked(scope, _rx_exact(s))
    else:
        await _click_link_in_section(scope, _rx_exact("Status"), _rx_exact("all"))

    # --- GRAD YEAR (unchanged) ---
    if grad_year:
        await _click_link_in_section(scope, _rx_exact("Grad. Year"), re.compile(r"^\s*none\s*$", re.I))
        rx_year = _rx_startswith(grad_year)
        await _scroll_until_visible(scope, rx_year)
        await ensure_checkbox_checked(scope, rx_year)

        
# ===================== EXPORT FLOW =====================

async def open_right_kebab_and_click_export(page):
    """
    Recruits page: click the 3-line (hamburger/kebab) menu, then choose 'Export'.
    Prefers the stable data-cy="export" hook when present.
    """
    import re, asyncio

    def _rx_exact(s: str):
        return re.compile(rf"^\s*{re.escape(s)}\s*$", re.I)

    # ---- find the hamburger/3-line trigger ---------------------------------
    # Primary candidates: obvious menu buttons on the toolbar
    trigger_selectors = [
        "button[aria-haspopup='menu']",
        "button[aria-label*='menu' i]",
        "button[title*='menu' i]",
        # generic icon-only buttons in toolbars
        "div[role='toolbar'] button",
        "header button",
    ]
    triggers = page.locator(",".join(trigger_selectors))

    # Special case: the icon itself has aria-label "Bulk Update Menu"
    # Click its nearest <button> ancestor if present.
    bulk_icon = page.locator("mat-icon[aria-label*='Bulk Update Menu' i]").first
    try:
        if await bulk_icon.count():
            btn_from_icon = bulk_icon.locator("xpath=ancestor::button[1]")
            triggers = triggers.union(btn_from_icon)  # add to the pool
    except AttributeError:
        # Playwright <1.49 has no .union(); just rely on the normal triggers.
        pass

    if await triggers.count() == 0:
        raise RuntimeError("Hamburger/3-line menu not found")

    # Prefer the right-most visible trigger (matches the toolbar kebab)
    right_idx, right_x = 0, -1
    for i in range(await triggers.count()):
        el = triggers.nth(i)
        try:
            await el.wait_for(timeout=1500)
            box = await el.bounding_box()
            if box and box["y"] < 4000 and box["x"] > right_x:  # ignore weird offscreen nodes
                right_x, right_idx = box["x"], i
        except:
            continue
    menu_btn = triggers.nth(right_idx)

    # ---- open the menu & click Export --------------------------------------
    async def _open_menu_and_click_export():
        await menu_btn.scroll_into_view_if_needed()
        await menu_btn.click(force=True)

        # Wait for an overlay panel that contains a mat menu OR any role=menu
        panel = page.locator(".cdk-overlay-pane:has(.mat-menu-content), [role='menu']").last
        await panel.wait_for(timeout=3000)

        # Try the most stable locator first, then fallbacks
        candidates = [
            panel.locator('[data-cy="export"]').first,                           # ❤️ your HTML
            panel.locator('button[role="menuitem"][data-cy="export"]').first,
            panel.get_by_role("menuitem", name=_rx_exact("Export")).first,
            panel.get_by_role("button",   name=_rx_exact("Export")).first,
            panel.locator(".mat-menu-content .mat-menu-item:has-text('Export')").first,
            panel.locator("text=Export").first,
        ]

        for el in candidates:
            try:
                if await el.count() and await el.is_visible():
                    await el.scroll_into_view_if_needed()
                    await el.click(timeout=1500)
                    await page.wait_for_load_state("networkidle")
                    return True
            except:
                continue

        # Debug aid: print what menu items we actually saw
        try:
            items = panel.locator("[role='menuitem'], .mat-menu-content .mat-menu-item, .mat-menu-content a")
            texts = []
            n = await items.count()
            for i in range(min(n, 20)):
                try:
                    t = (await items.nth(i).inner_text()).strip()
                    if t: texts.append(t)
                except:
                    pass
            if texts:
                print("[debug] hamburger menu items:", " | ".join(texts))
        except:
            pass

        # close menu to avoid stale overlay for the next attempt
        try: await page.keyboard.press("Escape")
        except: pass
        await asyncio.sleep(0.15)
        return False

    # Try up to 3 times (menus can lose focus in headless)
    for _ in range(3):
        if await _open_menu_and_click_export():
            return

    raise RuntimeError("Export option not found after opening menu")



async def open_export_and_start_job(layout_text: str, page):
    dropdown = None
    for loc in [
        page.locator("#exportLayout"),
        page.get_by_role("combobox").filter(has_text=re.compile("Export Layout|Layout", re.I)).first,
        page.get_by_role("button", name=re.compile(r"Export Layout|Select layout|Layout", re.I)).first,
        page.get_by_label(re.compile(r"Export Layout|Layout", re.I)).first,
    ]:
        try:
            await loc.wait_for(timeout=5000); dropdown = loc; break
        except: continue
    if not dropdown:
        raise RuntimeError("Export modal: layout dropdown not found.")

    await dropdown.scroll_into_view_if_needed(); await dropdown.click()
    picked = False
    for finder in [
        lambda: page.get_by_role("option",   name=_rx_exact(layout_text)).first,
        lambda: page.get_by_role("menuitem", name=_rx_exact(layout_text)).first,
        lambda: page.get_by_text(            _rx_exact(layout_text)).first,
    ]:
        try:
            await finder().click(timeout=5000); picked = True; break
        except: continue
    if not picked:
        raise RuntimeError(f"Export modal: layout '{layout_text}' not found.")

    export_btn_candidates = [
        page.get_by_role("button", name=re.compile(r"^\s*Export\b.*", re.I)).first,
        page.locator("button[type='submit']").first,
        page.locator("button.k-button--primary, button.mat-primary").filter(has_text=re.compile(r"^\s*Export\b", re.I)).first,
        page.get_by_text(re.compile(r"^\s*Export\b.*", re.I)).first,
    ]
    for btn in export_btn_candidates:
        try:
            await btn.scroll_into_view_if_needed()
            await page.wait_for_timeout(200)
            await btn.click(timeout=5000)
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(0.5)
            return
        except: continue
    raise RuntimeError("Export modal: could not find/click the Export button.")

async def maybe_go_to_exports_prompt(page):
    """
    If ARMS shows a post-start prompt, click the action to go to the Exports page.
    """
    for finder in [
        # exact text you showed
        lambda: page.get_by_role("button", name=re.compile(r"^\s*Take me to Exports page\s*$", re.I)).first,
        lambda: page.get_by_text(re.compile(r"^\s*Take me to Exports page\s*$", re.I)).first,

        # other tenants we’ve seen
        lambda: page.get_by_role("button", name=re.compile(r"^\s*Go to Exports\s*$", re.I)).first,
        lambda: page.get_by_role("link",   name=re.compile(r"^\s*Go to Exports\s*$", re.I)).first,
        lambda: page.get_by_text(re.compile(r"^\s*Go to Exports\s*$", re.I)).first,
        lambda: page.get_by_role("button", name=re.compile(r"^\s*Go to Export(s)? Page\s*$", re.I)).first,
        lambda: page.get_by_text(re.compile(r"^\s*Go to Export(s)? Page\s*$", re.I)).first,
    ]:
        try:
            await finder().click(timeout=2000)
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(0.3)
            return True
        except:
            continue
    return False

async def disable_auto_refresh_if_present(page):
    """
    On the Exports page there is a 'This page will auto-refresh' toggle.
    Turn it OFF so our locators stop getting invalidated.
    """
    try:
        # The text is next to a small refresh icon + a button; click the button if it's 'on'.
        block = page.locator("text=This page will auto-refresh").first
        await block.wait_for(timeout=2000)
        # The toggle is usually the next sibling button/menu
        container = block.locator("xpath=..")  # parent row
        toggle = container.locator("button, [role='button']").filter(has=page.locator("svg")).first
        # If there is an aria-pressed attribute and it's 'true', click to disable.
        pressed = await toggle.get_attribute("aria-pressed")
        if pressed is None or pressed.lower() == "true":
            await toggle.click(timeout=1500)
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(0.25)
    except:
        # Not fatal if we can't find it; continue.
        pass


async def fetch_latest_export_from_admin(page, layout_text: str, timeout_s: int = 180, skip_if_same=True):
    """
    On Administration → Exports:
      • Disable auto-refresh
      • Sort by 'Submit Date' newest→oldest (best-effort)
      • Find first row where 'File / Data' filename matches layout tokens AND Status == Complete
      • Click that link and download the CSV → DataFrame
    """
    tokens = _layout_tokens(layout_text)

    # Navigate to Exports if we didn't arrive via the prompt.
    url = page.url.lower()
    if "admin" not in url or "export" not in url:
        for step in [
            lambda: page.get_by_text(_rx_exact("Administration")).first.click(timeout=3000),
            lambda: page.get_by_role("link", name=re.compile(r"Administration", re.I)).first.click(timeout=3000),
        ]:
            try:
                await step(); await page.wait_for_load_state("networkidle"); break
            except: pass
        for step in [
            lambda: page.get_by_text(_rx_exact("Exports")).first.click(timeout=3000),
            lambda: page.get_by_role("link", name=re.compile(r"Exports", re.I)).first.click(timeout=3000),
        ]:
            try:
                await step(); await page.wait_for_load_state("networkidle"); break
            except: pass

    # Turn off the page auto-refresh if it's on
    await disable_auto_refresh_if_present(page)

    # Try to sort newest first by clicking "Submit Date" header
    try:
        submit_hdr = page.locator("table thead th").filter(
            has=page.get_by_text(re.compile(r"^\s*Submit\s*Date\s*$", re.I))
        ).first
        await submit_hdr.click(timeout=1500)
        await page.wait_for_load_state("networkidle")
        # click again to enforce desc if the first click sorted asc
        await submit_hdr.click(timeout=1500)
        await page.wait_for_load_state("networkidle")
    except:
        pass  # best-effort

    # Resolve the index of the "File / Data" column so we always click the right link
    file_col_idx = None
    try:
        ths = page.locator("table thead th"); n_th = await ths.count()
        for i in range(n_th):
            t = (await ths.nth(i).inner_text()).strip().lower()
            if "file" in t and "data" in t:
                file_col_idx = i
                break
    except:
        pass

    # Helper: scan the current DOM (no reloads) for newest Complete row matching tokens
    async def _find_newest_complete():
        body_rows = page.locator("table tbody tr")
        n = await body_rows.count()
        for i in range(n):  # top→down; after sort this should be newest→oldest
            row = body_rows.nth(i)
            try:
                # Status must contain 'Complete'
                await row.get_by_text(re.compile(r"\bComplete(d)?\b", re.I)).first.wait_for(timeout=250)
            except:
                continue

            # Get filename text from the File/Data cell
            if file_col_idx is not None:
                cell = row.locator("td").nth(file_col_idx)
                link = cell.locator("a").first
            else:
                link = row.locator("a").first  # fallback: first link in row

            try:
                fn = (await link.inner_text()).strip()
            except:
                continue

            if not fn or not _filename_matches_layout(fn, tokens):
                continue

            return row, link, fn
        return None

    # Poll up to timeout_s, but DO NOT reload the page (auto-refresh was disabled)
    end = asyncio.get_event_loop().time() + timeout_s
    found = None
    while asyncio.get_event_loop().time() < end:
        found = await _find_newest_complete()
        if found:
            break
        await asyncio.sleep(1.0)

    if not found:
        raise RuntimeError(f"Exports: no COMPLETE file found for layout '{layout_text}' within timeout.")

    row, link_el, filename = found

    # Optional skip-if-same logic
    cache = _read_cache() if skip_if_same else {}
    if skip_if_same and cache.get(layout_text) == filename:
        print(f"[info] latest file for '{layout_text}' already processed: {filename}")
        return pd.DataFrame()
    if skip_if_same:
        cache[layout_text] = filename
        _write_cache(cache)

    # Click the link and download (now that the page is stable)
    async with page.expect_download() as dl_ctx:
        await link_el.click()
    download = await dl_ctx.value

    with tempfile.TemporaryDirectory() as td:
        path = await download.path()
        if path is None:
            save_to = os.path.join(td, download.suggested_filename or filename or "export.csv")
            await download.save_as(save_to); path = save_to
        try:
            return pd.read_csv(path, dtype=str, encoding="utf-8-sig")
        except Exception:
            return pd.read_csv(path, dtype=str)
async def start_export_from_admin(layout_text: str, page):
    import re, asyncio

    # Administration → Exports
    for step in [
        lambda: page.get_by_role("link", name=re.compile(r"Administration", re.I)).first.click(timeout=3000),
        lambda: page.get_by_text(re.compile(r"^\s*Administration\s*$", re.I)).first.click(timeout=3000),
    ]:
        try:
            await step(); await page.wait_for_load_state("networkidle"); break
        except: pass
    for step in [
        lambda: page.get_by_role("link", name=re.compile(r"Exports", re.I)).first.click(timeout=3000),
        lambda: page.get_by_text(re.compile(r"^\s*Exports\s*$", re.I)).first.click(timeout=3000),
    ]:
        try:
            await step(); await page.wait_for_load_state("networkidle"); break
        except: pass

    # Try to open the Export menu (3-line "hamburger" or More button)
    for sel in [
        "button[aria-label*='Menu']",
        "button[title*='Menu']",
        "button:has(svg)",   # generic icon buttons
        "button:has-text('≡')",
        "button:has(.kebab), button:has(.hamburger)",
    ]:
        try:
            btn = page.locator(sel).first
            await btn.wait_for(timeout=5000)
            await btn.scroll_into_view_if_needed()
            await btn.click()
            await page.wait_for_timeout(1000)  # wait for dropdown to open
            break
        except Exception:
            continue
    else:
        raise RuntimeError("Hamburger/3-line menu not found")
    
    # Step 2: click the "Export" option from the dropdown
    for sel in [
        "text=Export",
        "button:has-text('Export')",
        "div[role='menu'] >> text=Export",
    ]:
        try:
            export_btn = page.locator(sel).first
            await export_btn.wait_for(timeout=5000)
            await export_btn.click()
            await page.wait_for_load_state("networkidle")
            break
        except Exception:
            continue
    else:
        raise RuntimeError("Export option not found after opening menu")
    
    # Choose layout
    dropdown = None
    for loc in [
        page.locator("#exportLayout"),
        page.get_by_role("combobox").filter(has_text=re.compile("Export Layout|Layout", re.I)).first,
        page.get_by_role("button", name=re.compile(r"Export Layout|Select layout|Layout", re.I)).first,
        page.get_by_label(re.compile(r"Export Layout|Layout", re.I)).first,
    ]:
        try:
            await loc.wait_for(timeout=4000); await loc.click(); dropdown = loc; break
        except: continue
    if not dropdown:
        raise RuntimeError("Admin Export: layout selector not found.")

    picked = False
    for finder in [
        lambda: page.get_by_role("option",   name=_rx_exact(layout_text)).first,
        lambda: page.get_by_role("menuitem", name=_rx_exact(layout_text)).first,
        lambda: page.get_by_text(            _rx_exact(layout_text)).first,
    ]:
        try:
            el = finder(); await el.scroll_into_view_if_needed(); await el.click(timeout=4000); picked = True;break
        except: continue
    if not picked:
        raise RuntimeError(f"Admin Export: layout '{layout_text}' not found.")

    # Click Export/Submit
    for btn in [
        page.get_by_role("button", name=re.compile(r"^\s*Export\b", re.I)).first,
        page.locator("button[type='submit']").first,
        page.locator("button.k-button--primary, button.mat-primary").filter(has_text=re.compile(r"^\s*Export\b", re.I)).first,
    ]:
        try:
            await btn.scroll_into_view_if_needed(); await page.wait_for_timeout(200)
            await btn.click(timeout=4000); await page.wait_for_load_state("networkidle"); return
        except: continue
    raise RuntimeError("Admin Export: could not click the final Export button.")

# ===================== CORE FLOW =====================
def _parse_grad_year(exp: Dict):
    f = exp.get("filters") or {}
    if "gradYear" in f:
        sel = f["gradYear"].get("selector", "")
        m = re.search(r"\b(20\d{2}|19\d{2})\b", sel) or re.search(r"\b(20\d{2}|19\d{2})\b", exp.get("name",""))
        return m.group(1) if m else None
    return None

async def do_one_export(page, exp: Dict):
    name = exp.get("name", "Unnamed")
    tab  = exp.get("tab")
    layout_text = exp.get("export", {}).get("layoutOptionText") or name.replace("_", " ")
    print(f"\n=== Export: {name} → Tab: {tab} ===", flush=True)

    # Close any open modal from prior run
    try:
        await page.get_by_role("button", name=_rx_exact("Cancel")).first.click(timeout=800)
        await page.wait_for_load_state("networkidle")
    except:
        pass

    await click_recruiting_recruits(page)

    scope = await find_filters_scope(page)
    try:
        await apply_filters(scope, _parse_grad_year(exp), _parse_statuses(exp))
    except Exception as e:
        print(f"[warn] filter step issue: {e}")

    # ✅ Primary + automatic fallback
    try:
        await open_right_kebab_and_click_export(page)
        await open_export_and_start_job(layout_text, page)
        await maybe_go_to_exports_prompt(page)
    except Exception as e:
        print(f"[warn] hamburger path failed: {e} — falling back to Admin → Exports")
        await start_export_from_admin(layout_text, page)

    # Download latest export and write to Sheets
    df = await fetch_latest_export_from_admin(page, layout_text, skip_if_same=False)
    if df is None or df.empty:
        print(f"[info] No new rows for '{layout_text}' (skipped).")
        return
    try:
        overwrite_tab(df, tab)
        print(f"[info] wrote {len(df):,} rows to '{tab}'")
    except Exception as e:
        print(f"[error] failed to write to Sheets for {name}: {e}")


async def run():
    cfg_path = Path(__file__).with_name("config.json")
    with cfg_path.open() as f:
        config = json.load(f)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=HEADLESS,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = await browser.new_context(accept_downloads=True, viewport={"width": 1366, "height": 900})
        page = await context.new_page()

        # Login
        print("[info] Logging into ARMS ...")
        await page.goto(ARMS_LOGIN_URL, wait_until="load")
        print("[debug] at URL:", page.url)
        
        # --- fill username/email
        try:
            await page.get_by_label(re.compile(r"Email|Username", re.I)).first.fill(ARMS_USER)
        except:
            await page.locator('input[type="email"], input[name*="user" i], input[type="text"]').first.fill(ARMS_USER)
        
        # click Next if present
        try:
            btn_next = page.get_by_role("button", name=_rx_exact("Next")).first
            if await btn_next.count():
                await btn_next.click()
                await page.wait_for_load_state("networkidle")
                await page.wait_for_timeout(800)
        except:
            pass
        
        # --- find password field (page or any iframe), then fill
        async def _find_password_locator():
            # main page first
            candidates = [
                page.get_by_label(re.compile(r"Password", re.I)).first,
                page.locator('input[type="password"]').first,
                page.locator('input[name*="pass" i]').first,
            ]
            for loc in candidates:
                try:
                    await loc.wait_for(timeout=6000)
                    return loc
                except:
                    pass
            # try frames
            for fr in page.frames:
                candidates = [
                    fr.get_by_label(re.compile(r"Password", re.I)).first,
                    fr.locator('input[type="password"]').first,
                    fr.locator('input[name*="pass" i]').first,
                ]
                for loc in candidates:
                    try:
                        await loc.wait_for(timeout=4000)
                        return loc
                    except:
                        pass
            return None
        
        pwd = await _find_password_locator()
        if not pwd:
            # tiny nudge: sometimes a second 'Next' or focus is needed
            try:
                await page.keyboard.press("Tab")
                await page.wait_for_timeout(400)
                pwd = await _find_password_locator()
            except:
                pass
        
        if not pwd:
            raise RuntimeError("Could not find password field after waiting")
        
        await pwd.fill(ARMS_PASS)
        
        # submit
        submitted = False
        for b in [
            page.get_by_role("button", name=re.compile(r"Sign in|Log in|Login", re.I)).first,
            page.locator('button[type="submit"]').first,
        ]:
            try:
                await b.click(timeout=4000)
                submitted = True
                break
            except:
                continue
        if not submitted:
            try:
                await pwd.press("Enter")
            except:
                pass
        
        await page.wait_for_load_state("networkidle")
        print("[info] Login complete.")


        for exp in config.get("exports", []):
            try:
                await do_one_export(page, exp)
            except Exception as e:
                print(f"[error] export failed for {exp.get('name','Unnamed')}: {e}")

        print("\n[done] All exports processed.")
        await context.close(); await browser.close()

if __name__ == "__main__":
    asyncio.run(run())

