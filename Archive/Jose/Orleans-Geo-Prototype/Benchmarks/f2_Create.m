fig1=figure;

ax=axes('FontSize', 22,'Parent',fig1);
xlabel('Number of Grains' ,'FontSize',22);
ylabel('Time (milli seconds)' ,'FontSize',22);
%title('HIT RATIO + Random Lookup + quorums-master-config-2-1-random-opt.txt');
hold on; % for matlab 6
X_labels = [1 5 10 20];
%axis(ax, [0 inf 0 inf])

var_0 = [6.979  15.132 28.824  55.156 ];
plot(X_labels, var_0, '-o', 'MarkerSize', 14 , 'LineWidth',  2);

var_2 = [8.505  19.880 38.262 75.162  ];
plot(X_labels, var_2, '-.+', 'MarkerSize', 14 , 'LineWidth',  2);

var_4 = [10.811  25.476 49.066 122.143 ];
plot(X_labels, var_4, '--*', 'MarkerSize', 14 , 'LineWidth',  2);

% var_3 = [0.5244 0.7364 0.8152 0.87 0.9262 0.943 0.9598 0.9696 0.9816];
% plot(X_labels, var_3, ':x', 'MarkerSize', 14 , 'LineWidth',  2);
% var_4 = [0.471 0.6516 0.7484 0.8306 0.8742 0.9188 0.943 0.9582 0.9682];
% plot(X_labels, var_4, '--s', 'MarkerSize', 14 , 'LineWidth',  2, 'MarkerFaceColor', [0 0 0], 'MarkerSize', 14);

leg = legend('Parallel Create - 1 Server', 'Parallel Create - 2 Servers', 'Parallel Create - 4 Servers');
set(leg, 'FontSize', 20, 'Location', 'NorthWest');
set(gca,'XTick', X_labels);
set(gca,'XTickLabel', {'1' '5'  '10' '20'});

print -depsc2 f2_Create.eps
print -djpeg  f2_Create.jpeg
