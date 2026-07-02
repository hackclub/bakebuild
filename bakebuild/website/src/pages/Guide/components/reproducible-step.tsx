import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

interface StepProps {
  children: ReactNode;
  className?: string;
}

export function Step({ children, className }: StepProps) {
  return <div className={cn("border-4 px-5 py-4", className)}>{children}</div>;
}

export function StepHeader({ children, className }: StepProps) {
  return <div className={cn("font-jaro text-4xl", className)}>{children}</div>;
}

export function StepContent({ children, className }: StepProps) {
  return (
    <div className={cn("grid gap-5 lg:grid-cols-2 text-lg pt-3", className)}>
      {children}
    </div>
  );
}

export default Step;
