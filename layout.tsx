import "./globals.css";

export const metadata = { title: "Trend Radar", description: "What is rising right now, by country." };

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
