import React from 'react';
import { it, expect, vi } from 'vitest';
import { renderTui } from './ui-test-helpers.js';
import { Text } from 'ink';
import { TerminalChatCommandReview } from '../src/components/chat/terminal-chat-command-review';
import { ReviewDecision } from '../src/utils/agent/review';

// Ensure that pressing 's' triggers the onSwitchApprovalMode callback
it('pressing s triggers onSwitchApprovalMode', async () => {
  const onSwitchApprovalMode = vi.fn();
  const onReviewCommand = vi.fn();
  const { stdin, flush } = renderTui(
    <TerminalChatCommandReview
      confirmationPrompt={<Text>Prompt</Text>}
      onReviewCommand={onReviewCommand}
      onSwitchApprovalMode={onSwitchApprovalMode}
    />
  );
  stdin.write('s');
  await flush();
  expect(onSwitchApprovalMode).toHaveBeenCalledTimes(1);
  expect(onReviewCommand).not.toHaveBeenCalled();
});

// Ensure that pressing 'y' triggers onReviewCommand with YES decision
it('pressing y triggers onReviewCommand with YES', async () => {
  const onReviewCommand = vi.fn();
  const onSwitchApprovalMode = vi.fn();
  const { stdin, flush } = renderTui(
    <TerminalChatCommandReview
      confirmationPrompt={<Text>Prompt</Text>}
      onReviewCommand={onReviewCommand}
      onSwitchApprovalMode={onSwitchApprovalMode}
    />
  );
  stdin.write('y');
  await flush();
  expect(onReviewCommand).toHaveBeenCalledWith(ReviewDecision.YES);
  expect(onSwitchApprovalMode).not.toHaveBeenCalled();
});