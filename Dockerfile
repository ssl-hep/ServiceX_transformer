FROM atlas/analysisbase:latest

USER atlas
RUN mkdir /home/atlas/servicex
COPY printXaodBranches.* /home/atlas/servicex/
CMD /home/atlas/servicex/printXaodBranches.sh
