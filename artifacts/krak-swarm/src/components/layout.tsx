import { ReactNode } from "react";
import { Link, useLocation } from "wouter";
import { 
  Activity, 
  Cpu, 
  Bot, 
  Dna, 
  ShieldAlert, 
  MessageSquare,
  Terminal
} from "lucide-react";
import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function Layout({ children }: { children: ReactNode }) {
  const [location] = useLocation();

  const navItems = [
    { href: "/", label: "OVERVIEW", icon: Activity },
    { href: "/engines", label: "RL_ENGINES", icon: Cpu },
    { href: "/bots", label: "SWARM_BOTS", icon: Bot },
    { href: "/mutations", label: "MUTATIONS", icon: Dna },
    { href: "/healing", label: "SELF_HEALING", icon: ShieldAlert },
    { href: "/telegram", label: "BRIDGE_LINK", icon: MessageSquare },
  ];

  return (
    <div className="min-h-screen w-full flex flex-col md:flex-row cyber-grid relative">
      {/* Scanline overlay */}
      <div className="pointer-events-none fixed inset-0 z-50 bg-[linear-gradient(rgba(18,16,16,0)_50%,rgba(0,0,0,0.25)_50%),linear-gradient(90deg,rgba(255,0,0,0.06),rgba(0,255,0,0.02),rgba(0,0,255,0.06))] bg-[length:100%_4px,3px_100%] opacity-10" />
      
      {/* Moving scan line */}
      <div className="pointer-events-none fixed inset-0 z-50 h-[10vh] bg-gradient-to-b from-transparent via-primary/5 to-transparent animate-scanline opacity-30" />

      {/* Sidebar */}
      <aside className="w-full md:w-64 border-b md:border-b-0 md:border-r border-primary/30 bg-card/80 backdrop-blur-md flex flex-col relative z-20">
        <div className="p-6 flex items-center gap-3 border-b border-primary/30">
          <Terminal className="w-8 h-8 text-primary" />
          <div>
            <h1 className="text-2xl font-bold text-primary tracking-widest text-glow leading-none">KRAK</h1>
            <p className="text-xs text-muted-foreground font-mono uppercase tracking-widest">Swarm.OS v2.4</p>
          </div>
        </div>

        <nav className="flex-1 py-6 px-4 space-y-2">
          {navItems.map((item) => {
            const isActive = location === item.href;
            return (
              <Link key={item.href} href={item.href} className="block w-full">
                <div className={cn(
                  "flex items-center gap-3 px-4 py-3 clip-edges transition-all duration-300 group cursor-pointer border",
                  isActive 
                    ? "bg-primary/10 border-primary text-primary box-glow" 
                    : "bg-transparent border-transparent text-muted-foreground hover:bg-primary/5 hover:border-primary/30 hover:text-primary-foreground"
                )}>
                  <item.icon className={cn(
                    "w-5 h-5 transition-transform duration-300 group-hover:scale-110",
                    isActive ? "text-primary drop-shadow-[0_0_8px_rgba(0,255,255,0.8)]" : ""
                  )} />
                  <span className="font-mono text-sm uppercase tracking-widest">
                    {item.label}
                  </span>
                  {isActive && (
                    <div className="ml-auto w-2 h-2 bg-primary animate-pulse-glow" />
                  )}
                </div>
              </Link>
            );
          })}
        </nav>

        <div className="p-4 border-t border-primary/30 text-xs font-mono text-muted-foreground/50 text-center">
          SYS.CONN: STABLE<br/>
          ENCRYPTION: AES-256-GCM
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 relative z-10 p-4 md:p-8 h-screen overflow-y-auto">
        <div className="max-w-7xl mx-auto">
          {children}
        </div>
      </main>
    </div>
  );
}
