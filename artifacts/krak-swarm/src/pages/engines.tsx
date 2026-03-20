import { useState } from "react";
import { 
  useListEngines, 
  useUpdateEngineParams, 
  useResetEngine,
  getListEnginesQueryKey
} from "@workspace/api-client-react";
import { useQueryClient } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle, CardFooter } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Cpu, Settings2, RotateCcw, AlertTriangle } from "lucide-react";
import type { Engine } from "@workspace/api-client-react/src/generated/api.schemas";
import { useToast } from "@/hooks/use-toast";

export default function Engines() {
  const { data, isLoading } = useListEngines();
  const [selectedEngine, setSelectedEngine] = useState<Engine | null>(null);

  if (isLoading) {
    return <div className="font-mono text-primary animate-pulse text-xl">[ SCANNING_ENGINES... ]</div>;
  }

  return (
    <div className="space-y-6">
      <header className="border-b border-primary/20 pb-4">
        <h2 className="text-3xl font-bold text-primary tracking-widest uppercase text-glow">RL_Engine_Cluster</h2>
        <p className="text-muted-foreground font-mono text-sm mt-1">
          TOTAL_INSTANCES: {data?.total} | ACTIVE_NODES: {data?.engines.filter(e => e.status === 'active').length}
        </p>
      </header>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
        {data?.engines.map((engine) => (
          <EngineCard key={engine.id} engine={engine} onEdit={() => setSelectedEngine(engine)} />
        ))}
      </div>

      {selectedEngine && (
        <EditEngineDialog 
          engine={selectedEngine} 
          open={!!selectedEngine} 
          onOpenChange={(isOpen) => !isOpen && setSelectedEngine(null)} 
        />
      )}
    </div>
  );
}

function EngineCard({ engine, onEdit }: { engine: Engine, onEdit: () => void }) {
  const { toast } = useToast();
  const queryClient = useQueryClient();
  const resetMutation = useResetEngine({
    mutation: {
      onSuccess: () => {
        toast({ title: "Engine Reset Sequence Complete", description: `${engine.name} restored to defaults.` });
        queryClient.invalidateQueries({ queryKey: getListEnginesQueryKey() });
      }
    }
  });

  const statusColors = {
    active: "bg-secondary/20 text-secondary border-secondary/50",
    idle: "bg-muted/50 text-muted-foreground border-muted",
    training: "bg-accent/20 text-accent border-accent/50",
    error: "bg-destructive/20 text-destructive border-destructive/50",
  };

  return (
    <Card className="bg-card/40 border-primary/30 hover:border-primary transition-colors flex flex-col group relative overflow-hidden">
      <div className="absolute inset-0 bg-gradient-to-br from-primary/5 to-transparent opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none" />
      <CardHeader className="pb-2 border-b border-primary/10">
        <div className="flex justify-between items-start">
          <div>
            <div className="text-xs font-mono text-muted-foreground mb-1">NODE_{engine.engineNumber.toString().padStart(3, '0')}</div>
            <CardTitle className="text-lg font-bold text-primary tracking-wide">{engine.name}</CardTitle>
          </div>
          <Badge variant="outline" className={`font-mono text-[10px] ${statusColors[engine.status]}`}>
            {engine.status}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="py-4 space-y-3 font-mono text-xs flex-1">
        <div className="flex justify-between">
          <span className="text-muted-foreground">TYPE</span>
          <span className="text-primary-foreground">{engine.type}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted-foreground">WIN_RATE</span>
          <span className={engine.winRate > 50 ? 'text-secondary' : 'text-destructive'}>
            {engine.winRate.toFixed(1)}%
          </span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted-foreground">LR</span>
          <span className="text-primary">{engine.learningRate}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted-foreground">EPSILON</span>
          <span className="text-primary">{engine.epsilon}</span>
        </div>
        <div className="flex justify-between pt-2 border-t border-primary/10 mt-2">
          <span className="text-muted-foreground">REWARD</span>
          <span className="text-primary-foreground font-bold">{engine.totalReward.toLocaleString()}</span>
        </div>
      </CardContent>
      <CardFooter className="pt-2 pb-4 gap-2 border-t border-primary/10">
        <Button variant="outline" size="sm" className="flex-1 font-mono text-xs border-primary/50 text-primary hover:bg-primary hover:text-primary-foreground" onClick={onEdit}>
          <Settings2 className="w-3 h-3 mr-2" /> EDIT_PARAMS
        </Button>
        <Button 
          variant="outline" 
          size="icon" 
          className="border-destructive/50 text-destructive hover:bg-destructive hover:text-destructive-foreground"
          onClick={() => resetMutation.mutate({ engineId: engine.id })}
          disabled={resetMutation.isPending}
        >
          {resetMutation.isPending ? <Activity className="w-4 h-4 animate-spin" /> : <RotateCcw className="w-4 h-4" />}
        </Button>
      </CardFooter>
    </Card>
  );
}

function EditEngineDialog({ engine, open, onOpenChange }: { engine: Engine, open: boolean, onOpenChange: (open: boolean) => void }) {
  const { toast } = useToast();
  const queryClient = useQueryClient();
  
  const [params, setParams] = useState({
    learningRate: engine.learningRate,
    gamma: engine.gamma,
    epsilon: engine.epsilon,
    batchSize: engine.batchSize
  });

  const mutation = useUpdateEngineParams({
    mutation: {
      onSuccess: () => {
        toast({ title: "Parameters Synced", description: "Engine neural paths updated successfully." });
        queryClient.invalidateQueries({ queryKey: getListEnginesQueryKey() });
        onOpenChange(false);
      }
    }
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    mutation.mutate({
      engineId: engine.id,
      data: {
        learningRate: Number(params.learningRate),
        gamma: Number(params.gamma),
        epsilon: Number(params.epsilon),
        batchSize: Number(params.batchSize)
      }
    });
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="bg-background border border-primary/50 shadow-[0_0_40px_rgba(0,255,255,0.15)] font-mono">
        <DialogHeader>
          <DialogTitle className="text-primary text-xl flex items-center gap-2 uppercase tracking-widest">
            <Settings2 className="w-5 h-5" />
            OVERRIDE_PARAMS :: {engine.name}
          </DialogTitle>
        </DialogHeader>
        <form onSubmit={handleSubmit} className="space-y-4 py-4">
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label className="text-muted-foreground text-xs">LEARNING_RATE</Label>
              <Input 
                type="number" step="0.0001" 
                className="bg-card border-primary/30 text-primary-foreground focus-visible:ring-primary focus-visible:border-primary"
                value={params.learningRate}
                onChange={e => setParams({...params, learningRate: e.target.value as any})}
              />
            </div>
            <div className="space-y-2">
              <Label className="text-muted-foreground text-xs">GAMMA_DISCOUNT</Label>
              <Input 
                type="number" step="0.01" 
                className="bg-card border-primary/30 text-primary-foreground focus-visible:ring-primary focus-visible:border-primary"
                value={params.gamma}
                onChange={e => setParams({...params, gamma: e.target.value as any})}
              />
            </div>
            <div className="space-y-2">
              <Label className="text-muted-foreground text-xs">EPSILON_DECAY</Label>
              <Input 
                type="number" step="0.01" 
                className="bg-card border-primary/30 text-primary-foreground focus-visible:ring-primary focus-visible:border-primary"
                value={params.epsilon}
                onChange={e => setParams({...params, epsilon: e.target.value as any})}
              />
            </div>
            <div className="space-y-2">
              <Label className="text-muted-foreground text-xs">BATCH_SIZE</Label>
              <Input 
                type="number" 
                className="bg-card border-primary/30 text-primary-foreground focus-visible:ring-primary focus-visible:border-primary"
                value={params.batchSize}
                onChange={e => setParams({...params, batchSize: e.target.value as any})}
              />
            </div>
          </div>
          
          <div className="bg-destructive/10 border border-destructive/30 p-3 rounded text-xs text-destructive flex gap-2 items-start mt-4">
            <AlertTriangle className="w-4 h-4 shrink-0 mt-0.5" />
            <p>Direct parameter mutation bypasses MAP-Elites evolution logic. Engine performance may become unstable.</p>
          </div>

          <DialogFooter className="mt-6">
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)} className="border-primary/30 text-primary hover:bg-primary/10">
              ABORT
            </Button>
            <Button type="submit" className="bg-primary text-primary-foreground hover:bg-primary/80 box-glow" disabled={mutation.isPending}>
              {mutation.isPending ? "INJECTING..." : "INJECT_MUTATION"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
