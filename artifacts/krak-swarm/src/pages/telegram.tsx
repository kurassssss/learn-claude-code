import { useState, useEffect } from "react";
import { 
  useGetTelegramConfig, 
  useUpdateTelegramConfig,
  usePushTelegramMessage 
} from "@workspace/api-client-react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription, CardFooter } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { MessageSquare, Send, Save, BellRing } from "lucide-react";
import { useToast } from "@/hooks/use-toast";

export default function Telegram() {
  const { data: config, isLoading } = useGetTelegramConfig();
  const { toast } = useToast();
  
  const [formData, setFormData] = useState({
    enabled: false,
    botToken: "",
    chatId: "",
    notifyOnConsensus: false,
    notifyOnError: false,
    notifyOnMutation: false,
    notifyOnHeal: false,
    minSeverity: "info"
  });

  const [pushMsg, setPushMsg] = useState("");
  const [pushPriority, setPushPriority] = useState<"low"|"normal"|"high"|"critical">("normal");

  useEffect(() => {
    if (config) {
      setFormData({
        enabled: config.enabled,
        botToken: config.botToken,
        chatId: config.chatId,
        notifyOnConsensus: config.notifyOnConsensus,
        notifyOnError: config.notifyOnError,
        notifyOnMutation: config.notifyOnMutation,
        notifyOnHeal: config.notifyOnHeal,
        minSeverity: config.minSeverity
      });
    }
  }, [config]);

  const updateMutation = useUpdateTelegramConfig({
    mutation: {
      onSuccess: () => toast({ title: "Config Saved", description: "Telegram bridge settings updated." })
    }
  });

  const pushMutation = usePushTelegramMessage({
    mutation: {
      onSuccess: () => {
        toast({ title: "Message Dispatched", description: "Payload transmitted to Telegram." });
        setPushMsg("");
      }
    }
  });

  const handleSaveConfig = (e: React.FormEvent) => {
    e.preventDefault();
    updateMutation.mutate({ data: formData });
  };

  const handlePush = (e: React.FormEvent) => {
    e.preventDefault();
    if (!pushMsg) return;
    pushMutation.mutate({ data: { message: pushMsg, priority: pushPriority } });
  };

  if (isLoading) return <div className="font-mono text-primary animate-pulse text-xl">[ INIT_COMM_LINK... ]</div>;

  return (
    <div className="space-y-6 max-w-4xl mx-auto">
      <header className="border-b border-primary/20 pb-4">
        <h2 className="text-3xl font-bold text-primary tracking-widest uppercase text-glow">Telegram_Bridge</h2>
        <p className="text-muted-foreground font-mono text-sm mt-1">
          SECURE NOTIFICATION RELAY & REMOTE COMMANDS
        </p>
      </header>

      <Card className="bg-card/40 border-primary/30 shadow-[0_0_30px_rgba(0,255,255,0.05)]">
        <form onSubmit={handleSaveConfig}>
          <CardHeader>
            <div className="flex justify-between items-center">
              <div>
                <CardTitle className="font-mono text-sm text-primary tracking-widest flex items-center gap-2">
                  <MessageSquare className="w-4 h-4" />
                  CONNECTION_PARAMS
                </CardTitle>
                <CardDescription className="font-mono text-xs mt-1">Configure bot credentials and routing ID.</CardDescription>
              </div>
              <div className="flex items-center gap-2 font-mono text-xs">
                <Label htmlFor="enabled-toggle" className={`${formData.enabled ? 'text-primary' : 'text-muted-foreground'}`}>
                  {formData.enabled ? 'ONLINE' : 'OFFLINE'}
                </Label>
                <Switch 
                  id="enabled-toggle" 
                  checked={formData.enabled} 
                  onCheckedChange={(v) => setFormData(f => ({...f, enabled: v}))} 
                  className="data-[state=checked]:bg-primary"
                />
              </div>
            </div>
          </CardHeader>
          <CardContent className="space-y-6 font-mono">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="space-y-2">
                <Label className="text-xs text-primary/70">BOT_TOKEN</Label>
                <Input 
                  type="password"
                  value={formData.botToken}
                  onChange={e => setFormData(f => ({...f, botToken: e.target.value}))}
                  className="bg-background border-primary/30 focus-visible:ring-primary font-mono text-sm"
                  placeholder="123456789:ABCDefGHIJKlmNOPQrsTUVwxyZ"
                />
              </div>
              <div className="space-y-2">
                <Label className="text-xs text-primary/70">TARGET_CHAT_ID</Label>
                <Input 
                  value={formData.chatId}
                  onChange={e => setFormData(f => ({...f, chatId: e.target.value}))}
                  className="bg-background border-primary/30 focus-visible:ring-primary font-mono text-sm"
                  placeholder="-1001234567890"
                />
              </div>
            </div>

            <div className="border-t border-primary/10 pt-6">
              <h3 className="text-xs text-primary uppercase mb-4 flex items-center gap-2">
                <BellRing className="w-3 h-3" /> Event Triggers
              </h3>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="flex items-center justify-between border border-primary/20 p-3 rounded bg-background/50">
                  <Label className="text-xs">MARKET_CONSENSUS</Label>
                  <Switch 
                    checked={formData.notifyOnConsensus} 
                    onCheckedChange={v => setFormData(f => ({...f, notifyOnConsensus: v}))} 
                    className="data-[state=checked]:bg-primary"
                  />
                </div>
                <div className="flex items-center justify-between border border-primary/20 p-3 rounded bg-background/50">
                  <Label className="text-xs">SYSTEM_ERRORS</Label>
                  <Switch 
                    checked={formData.notifyOnError} 
                    onCheckedChange={v => setFormData(f => ({...f, notifyOnError: v}))} 
                    className="data-[state=checked]:bg-destructive"
                  />
                </div>
                <div className="flex items-center justify-between border border-primary/20 p-3 rounded bg-background/50">
                  <Label className="text-xs">MUTATION_EVENTS</Label>
                  <Switch 
                    checked={formData.notifyOnMutation} 
                    onCheckedChange={v => setFormData(f => ({...f, notifyOnMutation: v}))} 
                    className="data-[state=checked]:bg-accent"
                  />
                </div>
                <div className="flex items-center justify-between border border-primary/20 p-3 rounded bg-background/50">
                  <Label className="text-xs">AUTO_HEAL_RESULTS</Label>
                  <Switch 
                    checked={formData.notifyOnHeal} 
                    onCheckedChange={v => setFormData(f => ({...f, notifyOnHeal: v}))} 
                    className="data-[state=checked]:bg-secondary"
                  />
                </div>
              </div>
            </div>
          </CardContent>
          <CardFooter className="bg-primary/5 border-t border-primary/20">
            <Button type="submit" disabled={updateMutation.isPending} className="ml-auto bg-primary text-primary-foreground hover:bg-primary/80 box-glow font-mono text-xs tracking-widest">
              {updateMutation.isPending ? "SAVING..." : <><Save className="w-4 h-4 mr-2" /> WRITE_CONFIG</>}
            </Button>
          </CardFooter>
        </form>
      </Card>

      <Card className="bg-card/40 border-primary/30">
        <form onSubmit={handlePush}>
          <CardHeader>
            <CardTitle className="font-mono text-sm text-primary tracking-widest flex items-center gap-2">
              <Send className="w-4 h-4" />
              MANUAL_DISPATCH
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4 font-mono text-sm">
            <div className="space-y-2">
              <Label className="text-xs text-primary/70">PRIORITY</Label>
              <Select value={pushPriority} onValueChange={(v: any) => setPushPriority(v)}>
                <SelectTrigger className="bg-background border-primary/30 focus:ring-primary w-48 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent className="bg-card border-primary/50 text-xs">
                  <SelectItem value="low" className="text-muted-foreground">LOW</SelectItem>
                  <SelectItem value="normal" className="text-primary">NORMAL</SelectItem>
                  <SelectItem value="high" className="text-accent">HIGH</SelectItem>
                  <SelectItem value="critical" className="text-destructive font-bold">CRITICAL</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label className="text-xs text-primary/70">PAYLOAD</Label>
              <Input 
                value={pushMsg}
                onChange={e => setPushMsg(e.target.value)}
                className="bg-background border-primary/30 focus-visible:ring-primary"
                placeholder="Enter string payload..."
                autoComplete="off"
              />
            </div>
          </CardContent>
          <CardFooter>
            <Button 
              type="submit" 
              disabled={pushMutation.isPending || !pushMsg}
              className="bg-transparent border border-primary text-primary hover:bg-primary hover:text-primary-foreground w-full font-mono text-xs tracking-widest"
            >
              {pushMutation.isPending ? "TRANSMITTING..." : "TRANSMIT_PAYLOAD"}
            </Button>
          </CardFooter>
        </form>
      </Card>

    </div>
  );
}
