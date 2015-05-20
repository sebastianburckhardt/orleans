fig1=figure;

ax=axes('FontSize', 22,'Parent',fig1); %, 'XTick', [100; 1000 ; 5000; 10000; 50000],'YTick', [0:0.1:1]);
xlabel('Message size (kilo bytes)' ,'FontSize',22);
ylabel('Time (milli seconds)' ,'FontSize',22);
%title('HIT RATIO + Random Lookup + quorums-master-config-2-1-random-opt.txt');
hold on; % for matlab 6
X_labels = [100 1000 5000 10000 50000];
%axis(ax, [-inf inf 0 2.3])

%var_1 = [0.954 1.014 1.069 1.208 2.539];
var_1 = [0.709 0.820 0.868 0.927 2.022];
plot(log2(X_labels), var_1, '-s', 'MarkerSize', 20 , 'LineWidth', 3, 'MarkerFaceColor', [0 0 0]);
%plot(log2(X_labels), var_1, '-.+', 'MarkerSize', 14 , 'LineWidth',  2);


%var_0 = [0.477 0.510 0.528 0.543 0.928];
var_0 = [0.366 0.375 0.385 0.393 0.669];
plot(log2(X_labels), var_0, '-o', 'MarkerSize', 20 , 'LineWidth', 3);

% var_2 = [0.5218 0.741 0.8506 0.9096 0.9424 0.9706 0.9792 0.9888 0.989];
% plot(X_labels, var_2, '--*', 'MarkerSize', 14 , 'LineWidth',  2);
% var_3 = [0.5244 0.7364 0.8152 0.87 0.9262 0.943 0.9598 0.9696 0.9816];
% plot(X_labels, var_3, ':x', 'MarkerSize', 14 , 'LineWidth',  2);
% var_4 = [0.471 0.6516 0.7484 0.8306 0.8742 0.9188 0.943 0.9582 0.9682];
% plot(X_labels, var_4, '--s', 'MarkerSize', 14 , 'LineWidth',  2, 'MarkerFaceColor', [0 0 0], 'MarkerSize', 14);

leg = legend('Between Servers','Same Server');
set(leg, 'FontSize', 22, 'Location', 'NorthWest');
set(gca,'XTick', log2(X_labels));
set(gca,'XTickLabel', {'0.1K' '1K'  '5K' '10K' '50K'});
print -depsc2 f1_ExchangeMessage.eps
print -djpeg f1_ExchangeMessage.jpeg


