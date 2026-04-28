export default function Footer() {
  return (
    <footer className="border-t bg-background/50 py-3 text-center text-xs text-muted-foreground">
      © 2026 · build {__APP_VERSION__}+{__BUILD_HASH__} · <a href="https://github.com/cnberg/marquee" target="_blank" rel="noopener noreferrer" className="hover:text-foreground hover:underline">GitHub</a>
    </footer>
  )
}
