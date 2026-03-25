export default function LandingVariationB() {
  const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Landing – Variation B: Spatial Polish + CTA Punch</title>
  <style>
    :root {
      --pb-white: #FFFFFF;
      --pb-accent: #5C7E8F;
      --pb-grey: #A2A2A2;
      --pb-lightgrey: #D4DDE2;
      --pb-text: #111111;
      --pb-text-on-accent: #FFFFFF;
      --font-small: 11px;
      --font-body: 13px;
      --font-brand: clamp(18px, 2vw, 22px);
      --font-heading: clamp(15px, 1.6vw, 18px);
      --line-body: 1.45;
      --line-heading: 1.25;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: Calibri, "Segoe UI", Arial, sans-serif;
      font-size: var(--font-body);
      line-height: var(--line-body);
      color: var(--pb-text);
      min-height: 100vh;
      background: var(--pb-white);
    }
    .small, .meta, .nav, .label { font-size: var(--font-small); }
    h1,h2,h3,h4,h5 { font-size: var(--font-heading); font-weight: 600; line-height: var(--line-heading); }

    .brand-lockup {
      display: flex;
      align-items: center;
      gap: 10px;
      text-decoration: none;
      color: inherit;
    }
    .brand-lockup img {
      height: 36px;
      width: 36px;
      object-fit: contain;
    }
    .brand-wordmark {
      font-size: var(--font-brand);
      font-weight: 700;
      letter-spacing: -0.3px;
      white-space: nowrap;
    }

    .landing-header {
      background: var(--pb-white);
      color: var(--pb-text);
      padding: 12px 24px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      position: sticky;
      top: 0;
      z-index: 100;
      border-bottom: 1px solid var(--pb-lightgrey);
      box-shadow: 0 2px 10px rgba(0,0,0,0.06);
    }
    .landing-header .brand-lockup {
      padding-bottom: 4px;
      border-bottom: 2px solid var(--pb-accent);
    }
    .landing-header .brand-wordmark { color: var(--pb-text); }
    .landing-header-right {
      display: flex;
      align-items: center;
      gap: 14px;
    }
    .landing-header-right a {
      color: var(--pb-text);
      text-decoration: none;
      font-size: var(--font-small);
      font-weight: 500;
      opacity: 0.8;
    }
    .landing-header-right .btn-header {
      background: var(--pb-grey);
      color: #fff;
      padding: 7px 16px;
      border-radius: 6px;
      font-weight: 600;
      font-size: var(--font-small);
      text-decoration: none;
      opacity: 1;
    }

    .landing-section {
      padding: 88px 32px;
    }
    .landing-section-inner {
      max-width: 740px;
      margin: 0 auto;
    }
    .section-white { background: var(--pb-white); color: var(--pb-text); }
    .section-accent { background: var(--pb-accent); color: var(--pb-text-on-accent); }

    .landing-section h2 {
      font-size: var(--font-heading);
      font-weight: 700;
      margin-bottom: 20px;
      line-height: var(--line-heading);
    }
    .landing-section h3 {
      font-size: var(--font-heading);
      font-weight: 600;
      margin-bottom: 14px;
      margin-top: 22px;
    }
    .landing-section p {
      font-size: var(--font-body);
      line-height: var(--line-body);
      margin-bottom: 14px;
    }
    .landing-section ul {
      list-style: none;
      padding: 0;
      margin-bottom: 14px;
    }
    .landing-section ul li {
      font-size: var(--font-body);
      line-height: 1.6;
      padding-left: 20px;
      position: relative;
      margin-bottom: 4px;
    }
    .section-white ul li::before {
      content: "\\2022";
      position: absolute;
      left: 6px;
      color: var(--pb-accent);
      font-weight: 700;
    }
    .section-accent ul li::before {
      content: "\\2022";
      position: absolute;
      left: 6px;
      color: var(--pb-lightgrey);
      font-weight: 700;
    }

    .btn-landing {
      display: inline-block;
      padding: 12px 28px;
      border-radius: 8px;
      font-size: var(--font-body);
      font-weight: 700;
      text-decoration: none;
      cursor: pointer;
      border: none;
    }
    .btn-primary-landing { background: var(--pb-accent); color: #fff; }
    .btn-secondary-landing { background: transparent; color: var(--pb-accent); border: 2px solid var(--pb-accent); margin-left: 12px; }
    .section-accent .btn-secondary-landing { color: #fff; border-color: #fff; }
    .section-accent .btn-primary-landing { background: #fff; color: var(--pb-accent); }

    .hero-section {
      padding: 88px 32px 80px;
      text-align: center;
    }
    .hero-section .landing-section-inner { max-width: 740px; }
    .hero-section h2 {
      font-size: var(--font-heading);
      margin-bottom: 14px;
    }
    .hero-section .hero-subtitle {
      font-size: var(--font-body);
      color: #555;
      margin-bottom: 10px;
      font-weight: 500;
    }
    .hero-section .hero-desc {
      font-size: var(--font-body);
      color: #666;
      max-width: 700px;
      margin: 0 auto 28px;
      line-height: var(--line-body);
    }
    .hero-buttons { display: flex; gap: 14px; justify-content: center; flex-wrap: wrap; }

    .callout-band {
      padding: 64px 32px;
      text-align: center;
    }
    .callout-band p {
      font-size: var(--font-heading);
      font-weight: 600;
      font-style: italic;
      max-width: 740px;
      margin: 0 auto;
      line-height: 1.4;
    }

    .cta-section {
      text-align: center;
      padding: 88px 32px;
    }
    .cta-section .landing-section-inner { max-width: 740px; }
    .cta-section h2 { margin-bottom: 24px; }
    .cta-section p { font-size: var(--font-body); margin-bottom: 8px; }

    .two-col {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 32px;
      margin-top: 20px;
    }

    .flow-diagram { margin-top: 32px; }
    .flow-diagram-title {
      text-align: center;
      font-size: var(--font-body);
      font-weight: 600;
      margin-bottom: 24px;
      font-style: italic;
    }
    .flow-step {
      max-width: 560px;
      margin: 0 auto;
      padding: 20px 24px;
      border-radius: 10px;
      position: relative;
    }
    .flow-step-white {
      background: #fff;
      color: var(--pb-text);
      border: 2px solid #e2e8f0;
    }
    .flow-step h4 {
      font-size: var(--font-body);
      font-weight: 700;
      margin-bottom: 6px;
    }
    .flow-step .flow-role {
      font-size: var(--font-small);
      font-weight: 600;
      color: var(--pb-accent);
      text-transform: uppercase;
      letter-spacing: 0.5px;
      margin-bottom: 4px;
    }
    .flow-step ul { margin: 6px 0 0 0; }
    .flow-step ul li { font-size: var(--font-body); line-height: 1.5; }
    .flow-step ul li::before { color: var(--pb-accent) !important; }
    .flow-arrow {
      text-align: center;
      padding: 6px 0;
      font-size: 24px;
      color: var(--pb-lightgrey);
      line-height: 1;
    }
    .flow-step-number {
      display: inline-block;
      background: var(--pb-accent);
      color: #fff;
      width: 26px;
      height: 26px;
      border-radius: 50%;
      text-align: center;
      line-height: 26px;
      font-size: var(--font-small);
      font-weight: 700;
      margin-right: 8px;
      vertical-align: middle;
    }

    .blog-placeholder {
      text-align: center;
      padding: 32px;
      border: 2px dashed rgba(255,255,255,0.3);
      border-radius: 12px;
      margin-top: 20px;
    }
    .blog-placeholder p { font-size: var(--font-body); opacity: 0.7; }

    .landing-footer {
      background: #1a1a2e;
      color: rgba(255,255,255,0.6);
      text-align: center;
      padding: 20px;
      font-size: var(--font-small);
    }
  </style>
</head>
<body>

<div class="landing-header">
  <a href="#" class="brand-lockup">
    <span class="brand-wordmark">Project Brainstorm</span>
  </a>
  <div class="landing-header-right">
    <a href="#">Blog / News</a>
    <a href="#">Login</a>
    <a href="#" class="btn-header">Sign Up</a>
  </div>
</div>

<!-- Section 1: Hero (white) -->
<section class="landing-section section-white hero-section">
  <div class="landing-section-inner">
    <h2>Simulate Markets. Avoid Costly Mistakes.</h2>
    <p class="hero-subtitle">AI&#8209;native market intelligence for Asia&#8209;Pacific and beyond.</p>
    <p class="hero-desc">Test strategies, messaging, and decisions with culturally grounded simulations — before committing capital.</p>
    <div class="hero-buttons">
      <a href="#" class="btn-landing btn-primary-landing">Run a Simulation</a>
      <a href="#" class="btn-landing btn-secondary-landing">See How It Works</a>
    </div>
  </div>
</section>

<!-- Section 2: The Problem (accent) -->
<section class="landing-section section-accent">
  <div class="landing-section-inner">
    <h2>The World's Most Important Region Is the Hardest to Understand</h2>
    <p>Asia&#8209;Pacific is fast&#8209;growing, culturally complex, and unforgiving.</p>
    <ul>
      <li>16+ major markets</li>
      <li>41+ languages</li>
      <li>Rapidly shifting consumers and regulations</li>
    </ul>
    <p>Yet most decisions are still made using:</p>
    <ul>
      <li>Slow, biased surveys</li>
      <li>Incomplete panels</li>
      <li>Gut feel and "experience"</li>
    </ul>
    <p>The result: cultural misfires, failed launches, and billions in lost value.</p>
  </div>
</section>

<!-- Section 3: The Insight Gap (white) -->
<section class="landing-section section-white">
  <div class="landing-section-inner">
    <h2>Traditional Research Is Too Slow. Generic AI Is Not Trustworthy.</h2>
    <div class="two-col">
      <div>
        <h3>Legacy market research</h3>
        <ul>
          <li>Takes weeks or months</li>
          <li>Is expensive and episodic</li>
          <li>Breaks down in APAC</li>
        </ul>
      </div>
      <div>
        <h3>Generic AI tools</h3>
        <ul>
          <li>Are fast but opaque</li>
          <li>Lack cultural depth and focus</li>
          <li>Cannot be audited or trusted</li>
        </ul>
      </div>
    </div>
    <p style="margin-top: 24px; font-weight: 600; font-style: italic; text-align: center;">Speed without trust isn't insight — it's risk.</p>
  </div>
</section>

<!-- Section 4: Our Solution (accent) -->
<section class="landing-section section-accent">
  <div class="landing-section-inner">
    <h2>AI&#8209;Native Market Intelligence Through Simulation</h2>
    <p>Project Brainstorm replaces traditional surveys and focus groups with grounded simulations using synthetic consumers.</p>
    <h3 style="color: var(--pb-lightgrey);">Synthetic consumers are:</h3>
    <ul>
      <li>Lifelike AI personas</li>
      <li>Statistically plausible representations of real people</li>
      <li>Grounded in trusted local journalism and regional data</li>
    </ul>
    <p>You interact through <strong>Mark</strong>, our Market Intelligence Copilot, who helps you:</p>
    <ul>
      <li>Frame the business problem</li>
      <li>Design the research</li>
      <li>Run simulations</li>
      <li>Turn results into decision&#8209;ready insights</li>
    </ul>
  </div>
</section>

<!-- Section 5: Who It's For (white) -->
<section class="landing-section section-white">
  <div class="landing-section-inner">
    <h2>Built for Teams That Can't Afford to Get APAC Wrong</h2>
    <ul>
      <li>Global and regional brand leaders</li>
      <li>Consultancies and agencies</li>
      <li>Investors and financial institutions</li>
      <li>Public sector and policy teams</li>
    </ul>
    <p style="margin-top: 18px; font-weight: 500;">If your decisions carry reputational, financial, or strategic risk — this is for you.</p>
  </div>
</section>

<!-- Section 6: Why Use Project Brainstorm (accent) -->
<section class="landing-section section-accent">
  <div class="landing-section-inner">
    <h2>Continuous Market Testing — Without the Friction</h2>
    <p>In a perfect world, teams would run constant research before every major decision.</p>
    <p>We make that possible by delivering:</p>
    <ul>
      <li><strong>Speed:</strong> insights in minutes, not weeks</li>
      <li><strong>Cultural fidelity:</strong> grounded in local sources, not assumptions</li>
      <li><strong>Auditability:</strong> every output is source&#8209;cited and explainable</li>
      <li><strong>Cost efficiency:</strong> SaaS subscriptions, not open&#8209;ended consulting</li>
    </ul>
    <p>Market research becomes a daily decision tool, not a last&#8209;minute expense.</p>
  </div>
</section>

<!-- Section 7: What Makes Us Different (white) -->
<section class="landing-section section-white">
  <div class="landing-section-inner">
    <h2>Trust Is Designed In — Not Assumed</h2>
    <p>Unlike chatbots, Project Brainstorm is not a black box.</p>
    <ul>
      <li>Every persona is grounded</li>
      <li>Every insight is labeled by confidence</li>
      <li>Every claim is traceable to real sources</li>
    </ul>
    <p style="font-weight: 500;">We don't ask you to "trust the model". We make trust verifiable.</p>
  </div>
</section>

<!-- Section 8: What You Get (accent) -->
<section class="landing-section section-accent">
  <div class="landing-section-inner">
    <h2>Familiar Research Outputs — Generated in a New Way</h2>
    <ul>
      <li>Synthetic survey results</li>
      <li>Synthetic focus group and in-depth interview transcripts</li>
      <li>Structured insight reports</li>
      <li>Recommendations, risks, and unknowns</li>
      <li>Full citations and audit trails</li>
    </ul>
    <p style="margin-top: 18px; font-weight: 500;">Same outputs decision&#8209;makers expect — delivered faster, cheaper, and with cultural precision.</p>
  </div>
</section>

<!-- Section 9: One-Line Differentiator (white, callout band) -->
<section class="landing-section section-white callout-band">
  <div class="landing-section-inner">
    <p>From human panels to grounded simulation — market research rebuilt for Asia&#8209;Pacific.</p>
  </div>
</section>

<!-- Section 10: Final CTA (accent) -->
<section class="landing-section section-accent cta-section">
  <div class="landing-section-inner">
    <h2>Make Better Decisions Before It's Too Late</h2>
    <p>Run a simulation.</p>
    <p>Test assumptions.</p>
    <p>Avoid blind spots.</p>
    <div style="margin-top: 28px;">
      <a href="#" class="btn-landing btn-primary-landing">Get Started With Mark</a>
    </div>
  </div>
</section>

<!-- Section 11: See How It Works (white) -->
<section class="landing-section section-white">
  <div class="landing-section-inner">
    <h2>See How It Works</h2>
    <p>You start with a real business decision, not a survey form.</p>
    <p><strong>Mark</strong> acts like a senior consultant — helping you frame the problem and choose the right research approach.</p>
    <p><strong>Lisa</strong> does the work — running culturally-grounded simulations using synthetic consumers.</p>
    <p><strong>Ben</strong> ensures trust — enforcing rigor, realism, and auditability before anything reaches you.</p>
    <p>You get insights you can act on, fast — with clarity on what's strong, what's tentative, and what's unknown.</p>

    <div class="flow-diagram">
      <p class="flow-diagram-title">Multi-agent ecosystem. One disciplined flow. No guesswork.</p>
      <div class="flow-step flow-step-white">
        <span class="flow-step-number">1</span>
        <h4 style="display: inline;">Your Business Question</h4>
      </div>
      <div class="flow-arrow">&#9660;</div>
      <div class="flow-step flow-step-white">
        <span class="flow-step-number">2</span>
        <div class="flow-role">Mark, Market Intelligence Copilot</div>
        <ul>
          <li>Clarifies the decision</li>
          <li>Frames the right questions</li>
          <li>Designs the study</li>
        </ul>
      </div>
      <div class="flow-arrow">&#9660;</div>
      <div class="flow-step flow-step-white">
        <span class="flow-step-number">3</span>
        <div class="flow-role">Lisa, Virtual Consultant</div>
        <ul>
          <li>Builds grounded personas</li>
          <li>Runs simulations (surveys, in-depth interviews, focus groups)</li>
        </ul>
      </div>
      <div class="flow-arrow">&#9660;</div>
      <div class="flow-step flow-step-white">
        <span class="flow-step-number">4</span>
        <div class="flow-role">Ben, QA Sentinel</div>
        <ul>
          <li>Checks realism and grounding</li>
          <li>Enforces rigor</li>
          <li>Blocks weak outputs</li>
        </ul>
      </div>
      <div class="flow-arrow">&#9660;</div>
      <div class="flow-step flow-step-white" style="border-color: var(--pb-accent);">
        <span class="flow-step-number">5</span>
        <div class="flow-role">Decision-ready insights</div>
        <ul>
          <li>Clear findings</li>
          <li>Confidence labels</li>
          <li>Full source citations</li>
          <li>Risks and unknowns</li>
        </ul>
      </div>
    </div>
  </div>
</section>

<!-- Section 12: Blog/News (accent) -->
<section class="landing-section section-accent">
  <div class="landing-section-inner">
    <h2>Blog / News</h2>
    <div class="blog-placeholder">
      <p>Coming soon</p>
    </div>
  </div>
</section>

<div class="landing-footer">
  &copy; 2026 Project Brainstorm. All rights reserved.
</div>

</body>
</html>`;

  return (
    <iframe
      srcDoc={html}
      style={{
        width: "100%",
        height: "100%",
        border: "none",
        display: "block",
      }}
      title="Variation B – Spatial Polish + CTA Punch"
    />
  );
}
